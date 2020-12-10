require "uri"
require "json"
require "socket"
require "uuid"
require "openssl"

# TODO: Write documentation for `NATS`
module NATS
  VERSION = "0.1.0"

  class Error < ::Exception
  end

  class NotAReply < Error
    getter nats_message : Message

    def initialize(error_message, @nats_message : Message)
      super error_message
    end
  end

  class UnknownCommand < Error
  end

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
    @current_sid = Atomic(Int64).new(0_i64)
    @subscriptions = {} of Int64 => Subscription
    @out = Mutex.new
    @ping_count = Atomic(Int32).new(0)
    @pings = Channel(Channel(Nil)).new(3) # For flushing the connection
    @disconnect_buffer = IO::Memory.new(capacity: 8 * MEGABYTE)
    @inbox_prefix = "_INBOX.#{Random::Secure.hex}"
    @inbox_handlers = {} of String => Proc(Message, Nil)
    getter state : State = :connecting
    getter? data_waiting = false

    def self.new(uri : URI, ping_interval = 2.minutes, max_pings_out = 2)
      new([uri], ping_interval: ping_interval, max_pings_out: max_pings_out)
    end

    def initialize(
      @servers = [URI.parse("nats:///")],
      @ping_interval : Time::Span = 2.minutes,
      @max_pings_out = 2,
    )
      uri = @servers.sample

      case uri.scheme
      when "nats"
        tls = false
      when "tls"
        tls = true
      else
        raise Error.new("Unknown URI scheme #{uri.scheme.inspect}, must be tls or nats")
      end
      default_port = tls ? 4443 : 4222
      s = TCPSocket.new(
        uri.host.presence || "localhost",
        uri.port || default_port,
      )
      s.tcp_nodelay = true
      s.sync = false
      s.read_buffering = true
      s.buffer_size = BUFFER_SIZE

      s.gets # server info

      if tls
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

      @socket << "CONNECT "
      {
        verbose: false,
        pedantic: false,
        lang: "Crystal",
        version: VERSION,
        protocol: 1,
        name: uri.path.sub(%r{\A/}, "").presence,
        user: uri.user,
        pass: uri.password,
      }.to_json @socket
      @socket << "\r\n"
      ping
      @socket.flush
      @socket.gets # pong
      @pings.receive
      subscribe "#{@inbox_prefix}.*" do |msg|
        if handler = @inbox_handlers[msg.subject]?
          handler.call msg
        end
      end

      if @state.reconnecting?
        IO.copy @disconnect_buffer, @socket
      else
        spawn begin_pings
        spawn begin_outbound
        spawn begin_inbound
      end

      @state = :connected

      ##### ALL SOCKET READS SHOULD BE DONE IN #begin_inbound PAST THIS POINT
      ##### NO DIRECT SOCKET READS PAST THIS POINT
    end

    def subscribe(subject : String, queue_group : String? = nil, &block : Message, Subscription ->) : Subscription
      sid = @current_sid.add 1

      write do
        @socket << "SUB " << subject << ' '
        if queue_group
          @socket << queue_group << ' '
        end
        @socket << sid << "\r\n"
      end

      @subscriptions[sid] = Subscription.new(subject, sid, &block).tap(&.start)
    end

    def unsubscribe(subscription : Subscription) : Nil
      unsubscribe subscription.sid
    end

    def unsubscribe(subscription : Subscription, max_messages : Int) : Nil
      unsubscribe subscription.sid, max_messages
    end

    def unsubscribe(sid : Int) : Nil
      write { @socket << "UNSUB " << sid << "\r\n" }
    ensure
      if subscription = @subscriptions.delete sid
        subscription.close
      end
    end

    def unsubscribe(sid : Int, max_messages : Int) : Nil
      write { @socket << "UNSUB " << sid << ' ' << max_messages << "\r\n" }
    ensure
      @subscriptions[sid].unsubscribe_after messages: max_messages
    end

    def request(subject : String, message : Data, timeout : Time::Span) : Message?
      channel = Channel(Message).new(1)
      inbox = Random::Secure.hex(4)
      key = "#{@inbox_prefix}.#{inbox}"
      @inbox_handlers[key] = ->(msg : Message) do
        channel.send msg
        @inbox_handlers.delete key
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

    def request(subject : String, message : Data, timeout : Time::Span, &block : Message ->) : Nil
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
        @socket << "PUB " << subject
        if reply_to
          @socket << ' ' << reply_to
        end
        @socket << ' ' << message.bytesize << "\r\n"
        @socket.write message.to_slice
        @socket << "\r\n"
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
        @socket << "PING\r\n"
        @ping_count.add 1
        @pings.send channel
      end
    end

    def pong
      write { @socket << "PONG\r\n" }
    end

    private def begin_pings
      loop do
        sleep @ping_interval
        return if @state.closed?
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
      rescue ex
        @on_error.call ex
      end
    end

    private def begin_inbound
      while line = @socket.gets
        case line
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
      end
    rescue ex : IO::Error
      unless @state.closed?
        @on_error.call ex
      end
    end

    def close
      flush
      @socket.close
      @state = :closed
    rescue IO::Error
    end

    @on_error = ->(error : Exception) {}
    def on_error(&@on_error : Exception ->) : Nil
    end

    private def write : Nil
      @out.synchronize do
        yield
        @data_waiting = true
      end
    rescue IO::Error
      @state = :disconnected
      reconnect!
    end

    private def reconnect!
      return unless @state.disconnected?
      @state = :reconnecting
      initialize(
        servers: @servers,
        ping_interval: @ping_interval,
        max_pings_out: @max_pings_out,
      )
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
    getter messages_remaining : Int32?
    private getter message_channel : MessageChannel

    def initialize(@subject, @sid, max_in_flight = 10, &@block : Message, Subscription ->)
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
