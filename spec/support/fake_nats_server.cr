require "socket"
require "json"

# A minimal in-process NATS server for exercising the client's connection
# handling. It speaks just enough of the protocol to complete the handshake,
# route PUB/HPUB to matching SUBs on any connection, honor UNSUB limits and
# answer PINGs. Tests can drop connections, refuse new ones, stop answering
# pings, or inject arbitrary protocol lines to simulate misbehaving servers.
class FakeNATSServer
  class Connection
    getter socket : TCPSocket
    # sid => subject pattern
    getter subscriptions = {} of String => String
    # sid => messages remaining before auto-unsubscribe
    getter limits = {} of String => Int32
    property? handshake_complete = false
    # The JSON payload of the client's CONNECT command
    property connect_options : JSON::Any?

    def initialize(@socket, @server : FakeNATSServer)
    end

    # Send an async INFO to the client, merging `extra` into the usual fields,
    # for example to announce lame-duck mode or new `connect_urls`.
    def send_info(**extra) : Nil
      send "INFO #{@server.info(**extra).to_json}"
    end

    # Send a raw protocol line to the client
    def send(line : String) : Nil
      @socket << line << "\r\n"
      @socket.flush
    end

    def close : Nil
      @socket.close rescue nil
    end
  end

  getter tcp_server : TCPServer
  getter connections = [] of Connection
  # Total number of connections ever accepted
  getter connection_count = 0
  # Whether to answer PINGs after the handshake completes
  property respond_to_pings = true
  # When false, new connections are accepted and immediately closed, which
  # looks to the client like a server that is down.
  property accepting = true

  def initialize
    @tcp_server = TCPServer.new("127.0.0.1", 0)
    spawn accept_loop
  end

  def port : Int32
    @tcp_server.local_address.port
  end

  def uri : URI
    URI.parse("nats://127.0.0.1:#{port}")
  end

  def close : Nil
    @tcp_server.close
    drop_connections
  end

  # Drop every current client connection, as if the server crashed
  def drop_connections : Nil
    @connections.dup.each(&.close)
    @connections.clear
  end

  def wait_for_connections(count : Int, timeout = 2.seconds) : Nil
    wait_for(timeout, "#{count} connections (got #{@connection_count})") { @connection_count >= count }
  end

  # Whether a subject matches a subscription pattern, supporting the `*` and
  # `>` wildcards.
  def self.matches?(pattern : String, subject : String) : Bool
    pattern_tokens = pattern.split('.')
    subject_tokens = subject.split('.')
    pattern_tokens.each_with_index do |token, i|
      return true if token == ">"
      subject_token = subject_tokens[i]?
      return false unless subject_token && (token == "*" || token == subject_token)
    end
    pattern_tokens.size == subject_tokens.size
  end

  private def accept_loop
    while socket = @tcp_server.accept?
      @connection_count += 1
      if accepting
        connection = Connection.new(socket, self)
        @connections << connection
        spawn serve(connection)
      else
        socket.close
      end
    end
  end

  # The fields of the INFO this server sends, with `extra` merged in
  def info(**extra)
    {
      server_id:   "fake",
      server_name: "fake",
      version:     "0.0.0",
      go:          "none",
      host:        "127.0.0.1",
      port:        port,
      headers:     true,
      max_payload: 1_048_576,
      proto:       1,
    }.merge(extra)
  end

  private def serve(connection : Connection)
    socket = connection.socket
    socket << "INFO " << info.to_json << "\r\n"
    socket.flush

    while line = socket.gets(chomp: true)
      parts = line.split(' ')
      case parts[0]?
      when "CONNECT"
        connection.connect_options = JSON.parse(line.lchop("CONNECT "))
      when "PONG", "+OK"
        # nothing to do
      when "PING"
        if !connection.handshake_complete? || respond_to_pings
          connection.send "PONG"
        end
        connection.handshake_complete = true
      when "SUB"
        # SUB subject [queue] sid
        connection.subscriptions[parts.last] = parts[1]
      when "UNSUB"
        # UNSUB sid [max]
        if max = parts[2]?
          connection.limits[parts[1]] = max.to_i
        else
          connection.subscriptions.delete parts[1]
          connection.limits.delete parts[1]
        end
      when "PUB"
        # PUB subject [reply] size
        reply = parts[2] if parts.size == 4
        payload = Bytes.new(parts.last.to_i)
        socket.read_fully payload
        socket.skip 2
        deliver parts[1], reply, payload
      when "HPUB"
        # HPUB subject [reply] header_size total_size
        reply = parts[2] if parts.size == 5
        payload = Bytes.new(parts.last.to_i)
        socket.read_fully payload
        socket.skip 2
        deliver parts[1], reply, payload, header_size: parts[-2].to_i
      else
        connection.send "-ERR 'Unknown Protocol Operation'"
      end
    end
  rescue IO::Error
  ensure
    @connections.delete connection
    connection.close
  end

  private def deliver(subject : String, reply : String?, payload : Bytes, header_size : Int32? = nil)
    @connections.dup.each do |connection|
      connection.subscriptions.dup.each do |sid, pattern|
        next unless self.class.matches?(pattern, subject)

        io = connection.socket
        io << (header_size ? "HMSG " : "MSG ") << subject << ' ' << sid
        io << ' ' << reply if reply
        io << ' ' << header_size if header_size
        io << ' ' << payload.size << "\r\n"
        io.write payload
        io << "\r\n"
        io.flush

        if remaining = connection.limits[sid]?
          remaining -= 1
          if remaining <= 0
            connection.subscriptions.delete sid
            connection.limits.delete sid
          else
            connection.limits[sid] = remaining
          end
        end
      rescue IO::Error
        # Connection went away mid-delivery
      end
    end
  end
end

# Poll until the block returns truthy, raising if it takes longer than `timeout`
def wait_for(timeout = 2.seconds, description = "condition", &)
  start = Time.instant
  until yield
    if start.elapsed > timeout
      raise "Timed out after #{timeout} waiting for #{description}"
    end
    sleep 1.millisecond
  end
end
