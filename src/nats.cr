require "uri"
require "json"
require "socket"
require "uuid"
require "openssl"
require "log"

# NATS is a pub/sub message bus.
#
# ```
# require "nats"
#
# # Connect to a NATS server running on localhost
# nats = NATS::Client.new
#
# # Connect to a single remote NATS server
# nats = NATS::Client.new(URI.parse(ENV["NATS_URL"]))
#
# # Connect to a NATS cluster, specified by the NATS_URLS environment variable
# # as a comma-separated list of URLs
# servers = ENV["NATS_URLS"]
#   .split(',')
#   .map { |url| URI.parse(url) }
# nats = NATS::Client.new(servers: %w[
#   nats://nats-1
#   nats://nats-2
#   nats://nats-3
# ])
# ```
module NATS
  VERSION = "0.1.0"

  # Generic error
  class Error < ::Exception
  end

  # Raised when trying to reply to a NATS message that is not a reply.
  class NotAReply < Error
    getter nats_message : Message

    def initialize(error_message, @nats_message : Message)
      super error_message
    end
  end

  class ServerNotRespondingToPings < Error
  end

  class UnknownCommand < Error
  end

  struct ServerInfo
    include JSON::Serializable

    getter server_id : String
    getter server_name : String
    getter version : String
    getter proto : Int32
    getter host : String
    getter port : Int32
    getter? headers : Bool = false
    getter? tls_required : Bool = false
    getter max_payload : Int32
    getter client_id : Int32
    getter client_ip : String
    getter? auth_required : Bool = false
    getter nonce : String?
    getter cluster : String?
    getter connect_urls : Array(String) = [] of String
  end

  LOG = ::Log.for(self)

  # Instantiating a `NATS::Client` makes a connection to one of the given NATS
  # servers.
  class Client
    alias Data = String | Bytes

    BUFFER_SIZE = 1 << 15
    MEGABYTE = 1 << 20
    MAX_PUBLISH_SIZE = 1 * MEGABYTE

    enum State
      Connecting
      Connected
      Disconnected
      Reconnecting
      Closed
    end

    @socket : TCPSocket | OpenSSL::SSL::Socket::Client
    @io : IO
    @current_sid = Atomic(Int64).new(0_i64)
    @subscriptions = {} of Int64 => Subscription
    @out = Mutex.new(protection: :reentrant)
    @ping_count = Atomic(Int32).new(0)
    @pings = Channel(Channel(Nil)).new(3) # For flushing the connection
    @disconnect_buffer = IO::Memory.new
    @inbox_prefix = "_INBOX.#{Random::Secure.hex}"
    @inbox_handlers = {} of String => Proc(Message, Nil)
    getter state : State = :connecting
    getter? data_waiting = false
    getter server_info : ServerInfo

    # Connect to a single NATS server at the given URI
    #
    # ```
    # nats = NATS::Client.new(URI.parse("nats://nats.example.com"))
    # ```
    def self.new(
      uri : URI,
      ping_interval = 2.minutes,
      max_pings_out = 2,
    )
      new([uri], ping_interval: ping_interval, max_pings_out: max_pings_out)
    end

    # Connect to a NATS cluster at the given URIs
    #
    # ```
    # nats = NATS::Client.new([
    #   URI.parse("nats://nats-1.example.com"),
    #   URI.parse("nats://nats-2.example.com"),
    #   URI.parse("nats://nats-3.example.com"),
    # ])
    # ```
    def initialize(
      @servers = [URI.parse("nats:///")],
      @ping_interval : Time::Span = 2.minutes,
      @max_pings_out = 2,
    )
      uri = @servers.sample
      @ping_count = Atomic.new(0)
      @pings = Channel(Channel(Nil)).new(3) # For flushing the connection
      @inbox_handlers = {} of String => Proc(Message, Nil)

      case uri.scheme
      when "nats"
        tls = false
      when "tls"
        tls = true
      else
        raise Error.new("Unknown URI scheme #{uri.scheme.inspect}, must be tls:// or nats://")
      end
      default_port = tls ? 4443 : 4222
      host = uri.host.presence || "localhost"
      port = uri.port || default_port
      LOG.debug { "Connecting to #{host}:#{port}..." }
      s = TCPSocket.new(host, port)
      s.tcp_nodelay = true
      s.sync = false
      s.read_buffering = true
      s.buffer_size = BUFFER_SIZE

      info_line = s.read_line
      LOG.debug { "RECEIVED: #{info_line}" }
      @server_info = ServerInfo.from_json info_line[5..-1]

      if tls || @server_info.tls_required?
        context = OpenSSL::SSL::Context::Client.new
        context.add_options(
          OpenSSL::SSL::Options::ALL |       # various workarounds
          OpenSSL::SSL::Options::NO_SSL_V2 | # disable overly deprecated SSLv2
          OpenSSL::SSL::Options::NO_SSL_V3   # disable deprecated SSLv3
        )
        s = OpenSSL::SSL::Socket::Client.new(s, context)
        s.sync = false
        s.read_buffering = true
      end

      @socket = s
      @io = s

      @io << "CONNECT "
      connect = {
        verbose: false,
        pedantic: false,
        lang: "Crystal",
        version: VERSION,
        protocol: 1,
        name: uri.path.sub(%r{\A/}, "").presence,
        user: uri.user,
        pass: uri.password,
      }
      connect.to_json @io
      @io << "\r\n"
      ping
      @socket.flush
      until (line = @socket.gets) == "PONG"
        # TODO: Handle errors
      end
      @pings.receive

      if @state.reconnecting?
        subscriptions = @subscriptions
        @subscriptions = {} of Int64 => Subscription
        subscriptions.each_value do |subscription|
          LOG.debug { "Resubscribing to subscription #{subscription.subject}#{" (queue_group: #{subscription.queue_group}}" if subscription.queue_group} on subscription id #{subscription.sid}..." }
          resubscribe subscription
        end
        IO.copy @disconnect_buffer.rewind, @io
        @socket.flush
        @disconnect_buffer.clear
      else
        spawn begin_pings
        spawn begin_outbound
        spawn begin_inbound
      end

      unless @state.reconnecting?
        inbox_subject = "#{@inbox_prefix}.*"
        LOG.debug { "Subscribing to inbox: #{inbox_subject}" }
        subscribe inbox_subject do |msg|
          if handler = @inbox_handlers[msg.subject]?
            handler.call msg
          end
        end
      end

      @state = :connected

      ##### ALL SOCKET READS SHOULD BE DONE IN #begin_inbound PAST THIS POINT
      ##### NO DIRECT SOCKET READS PAST THIS POINT
    end

    # Subscribe to the given `subject`, optionally with a `queue_group` (so that
    # each message is delivered to this application once instead of once for
    # each instance of the application), executing the given block for each
    # message.
    #
    # ```
    # require "nats"
    #
    # nats = NATS::Client.new
    # nats.subscribe "orders.created" do |msg|
    #   order = Order.from_json(msg.body_io)
    #
    #   # ...
    # end
    # ```
    def subscribe(subject : String, queue_group : String? = nil, sid = @current_sid.add(1), &block : Message, Subscription ->) : Subscription
      write do
        @io << "SUB " << subject << ' '
        if queue_group
          @io << queue_group << ' '
        end
        @io << sid << "\r\n"
      end

      @subscriptions[sid] = Subscription.new(subject, sid, queue_group, &block).tap(&.start)
    end

    private def resubscribe(subscription : Subscription)
      subscribe subscription.subject, subscription.queue_group, subscription.sid, &subscription.@block
    end

    # Unsubscribe from the given subscription
    #
    # ```
    # nats = NATS::Client.new
    #
    # new_orders = [] of NATS::Message
    # subscription = nats.subscribe "orders.created.*" do |msg|
    #   messages << msg
    # end
    #
    # spawn do
    #   sleep 10.seconds
    #   nats.unsubscribe subscription
    # end
    # ```
    def unsubscribe(subscription : Subscription) : Nil
      unsubscribe subscription.sid
    end

    # Unsubscribe from the given subscription after the specified number of messages has been received.
    def unsubscribe(subscription : Subscription, max_messages : Int) : Nil
      unsubscribe subscription.sid, max_messages
    end

    def unsubscribe(sid : Int) : Nil
      write { @io << "UNSUB " << sid << "\r\n" }
    ensure
      if subscription = @subscriptions.delete sid
        subscription.close
      end
    end

    def unsubscribe(sid : Int, max_messages : Int) : Nil
      write { @io << "UNSUB " << sid << ' ' << max_messages << "\r\n" }
    ensure
      @subscriptions[sid].unsubscribe_after messages: max_messages
    end

    # Make a synchronous request to subscribers of the given `subject`, waiting
    # up to `timeout` for a response from any of the subscribers. The first
    # message to come back will be returned. If no messages comes back before
    # the `timeout` elapses, `nil` is returned.
    #
    # ```
    # if order_response = nats.request("orders.info.#{order_id}")
    #   response << Order.from_json(order_response.body_io)
    # else
    #   response.status = :service_unavailable
    # end
    # ```
    def request(subject : String, message : Data = "", timeout : Time::Span = 2.seconds) : Message?
      channel = Channel(Message).new(1)
      inbox = Random::Secure.hex(4)
      key = "#{@inbox_prefix}.#{inbox}"
      @inbox_handlers[key] = ->(msg : Message) do
        @inbox_handlers.delete key
        channel.send msg
      end
      publish subject, message, reply_to: key

      select
      when msg = channel.receive
        msg
      when timeout(timeout)
        @inbox_handlers.delete key
        nil
      end
    end

    # Make an asynchronous request to subscribers of the given `subject`, not
    # waiting for a response. The first message to come back will be passed to
    # the block.
    def request(subject : String, message : Data = "", timeout = 2.seconds, &block : Message ->) : Nil
      inbox = Random::Secure.hex(4)
      key = "#{@inbox_prefix}.#{inbox}"
      @inbox_handlers[key] = ->(msg : Message) do
        block.call msg
        @inbox_handlers.delete key
      end
      publish subject, message, reply_to: key

      spawn remove_key(key, after: timeout)
    end

    private def remove_key(key, after timeout)
      sleep timeout
      @inbox_handlers.delete key
    end

    def reply(msg : Message, body : Data) : Nil
      if subject = msg.reply_to
        publish subject, body
      else
        raise NotAReply.new("Cannot reply to a message that has no return address", msg)
      end
    end

    def publish(subject : String, message : Data, reply_to : String? = nil) : Nil
      if message.bytesize > MAX_PUBLISH_SIZE
        raise Error.new("Attempted to publish message of size #{message.bytesize}. Cannot publish messages larger than #{MAX_PUBLISH_SIZE}.")
      end

      write do
        @io << "PUB " << subject
        if reply_to
          @io << ' ' << reply_to
        end
        @io << ' ' << message.bytesize << "\r\n"
        @io.write message.to_slice
        @io << "\r\n"
      end
    end

    def flush(timeout = 2.seconds)
      channel = Channel(Nil).new(1)
      ping channel
      @socket.flush

      Fiber.yield

      select
      when channel.receive
      when timeout(timeout)
        raise Error.new("Flush did not complete within duration: #{timeout}")
      end
    end

    def ping(channel = Channel(Nil).new(1))
      write do
        @io << "PING\r\n"
        @ping_count.add 1
        @pings.send channel
      end
    end

    def pong
      write { @io << "PONG\r\n" }
    end

    def jetstream
      @jetstream ||= JetStream::Client.new(self)
    end

    private def begin_pings
      loop do
        sleep @ping_interval
        return if @state.closed?
        if @ping_count.get > @max_pings_out
          handle_disconnect!
        end
        ping
      end
    end

    MAX_OUTBOUND_INTERVAL = 10.milliseconds
    @outbound_interval : Time::Span = 5.microseconds
    private def begin_outbound
      loop do
        sleep @outbound_interval
        return if @state.closed?

        if data_waiting?
          @out.synchronize do
            @socket.flush
            @data_waiting = false
            @outbound_interval = 5.microseconds
          end
        else
          @outbound_interval = {@outbound_interval * 2, MAX_OUTBOUND_INTERVAL}.min
        end
      rescue ex : IO::Error
        break if state.closed?
        @outbound_interval = MAX_OUTBOUND_INTERVAL
      rescue ex
        @on_error.call ex
      end
    end

    private def begin_inbound
      backoff = 1
      loop do
        if @socket.closed?
          break if state.closed?
          handle_inbound_disconnect IO::Error.new, backoff: backoff.milliseconds
        end

        line = @socket.gets
        break if state.closed?
        case line
        when Nil
          backoff *= 2
        when .starts_with? "MSG"
          starting_point = 4 # "MSG "
          if (subject_end = line.index(' ', starting_point)) && (sid_end = line.index(' ', subject_end + 1))
            subject = line[starting_point...subject_end]
            sid = line[subject_end + 1...sid_end].to_i

            # Figure out if we got a reply_to and set it and bytesize accordingly
            reply_to_with_byte_size = line[sid_end + 1..-1]
            if boundary = reply_to_with_byte_size.index(' ')
              reply_to = reply_to_with_byte_size[0...boundary]
              bytesize = reply_to_with_byte_size[boundary + 1..-1].to_i
            else
              bytesize = reply_to_with_byte_size.to_i
            end
          else
            raise Error.new("Invalid message declaration: #{line}")
          end

          body = Bytes.new(bytesize)
          @socket.read_fully?(body) || raise Error.new("Unexpected EOF")
          @socket.skip 2 # CRLF

          if subscription = @subscriptions[sid]?
            subscription.send Message.new(subject, body, reply_to: reply_to) do |ex|
              @on_error.call ex
            end
            if (messages_remaining = subscription.messages_remaining) && messages_remaining <= 0
              @subscriptions.delete sid
              subscription.close
            end
          end
        when "+OK"
          # Cool, thanks
        when "PING"
          pong
        when "PONG"
          if @ping_count.sub(1) >= 0
            select
            when ping = @pings.receive
              ping.send nil
            else
            end
          else
            raise Error.new("Received PONG without sending a PING")
          end
        when .starts_with? "-ERR"
          @on_error.call Error.new(line)
        else
          @on_error.call UnknownCommand.new(line)
        end
        backoff = 1
        Fiber.yield
      rescue ex : IO::Error
        break if state.closed?
        handle_inbound_disconnect ex, backoff: backoff.milliseconds
      end
    end

    def handle_inbound_disconnect(exception, backoff : Time::Span)
      LOG.debug { "Exception in inbound data handler: #{exception}" }
      exception.backtrace.each do |line|
        LOG.debug { line }
      end
      sleep backoff
    end

    def close
      return if @state.closed?
      flush
      @socket.close
      @state = :closed
    rescue IO::Error
    end

    @on_error = ->(error : Exception) {}
    def on_error(&@on_error : Exception -> Nil)
      self
    end

    @on_disconnect = -> {}
    def on_disconnect(&@on_disconnect)
      self
    end

    @on_reconnect = -> {}
    def on_reconnect(&@on_reconnect)
      self
    end

    private def write : Nil
      if @socket.closed?
        handle_disconnect!
      end

      loop do
        @out.synchronize do
          yield
          @data_waiting = true
        end
        return
      rescue ex : IO::Error
        handle_disconnect!
      end
    end

    private def handle_disconnect!
      loop do
        @out.synchronize do
          unless @state.closed? || @state.connecting?
            @socket.close unless @socket.closed?
            @state = :disconnected
            @on_disconnect.call
            # Redirect all writes to the buffer until we reconnect to the server
            @io = @disconnect_buffer
            reconnect!
          end
        end

        break
      rescue ex
        spawn @on_error.call(ex)
      end
    end

    private def reconnect!
      return unless @state.disconnected?
      @state = :reconnecting
      initialize(
        servers: @servers,
        ping_interval: @ping_interval,
        max_pings_out: @max_pings_out,
      )
      @on_reconnect.call
    end
  end

  struct Message
    getter subject : String
    getter body : Bytes
    getter reply_to : String?

    def initialize(@subject, @body, @reply_to = nil)
    end

    def body_io
      @body_io ||= IO::Memory.new(@body)
    end
  end

  class Subscription
    alias MessageChannel = Channel({Message, Proc(Exception, Nil)})

    getter subject : String
    getter sid : Int64
    getter queue_group : String?
    getter messages_remaining : Int32?
    private getter message_channel : MessageChannel

    def initialize(@subject, @sid, @queue_group, max_in_flight : Int = 10, &@block : Message, Subscription ->)
      @message_channel = MessageChannel.new(max_in_flight)
    end

    def unsubscribe_after(messages @messages_remaining : Int32)
    end

    def start
      spawn do
        remaining = @messages_remaining
        while remaining.nil? || remaining > 0
          message, on_error = message_channel.receive

          call message, on_error

          remaining = @messages_remaining
        end
      rescue ex
      end
    end

    def close
      @message_channel.close
    end

    def send(message, &on_error : Exception ->) : Nil
      message_channel.send({message, on_error})
    end

    private def call(message, on_error : Exception ->) : Nil
      @block.call message, self
    rescue ex
      on_error.call ex
    ensure
      if remaining = @messages_remaining
        @messages_remaining = remaining - 1
      end
    end
  end
end
