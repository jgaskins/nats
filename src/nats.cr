require "uri"
require "json"
require "socket"
require "uuid"

# TODO: Write documentation for `NATS`
module NATS
  VERSION = "0.1.0"

  class Error < ::Exception
  end

  class Client
    alias Data = String | Bytes

    BUFFER_SIZE = 1 << 15
    MAX_PUBLISH_SIZE = 1_000_000

    @current_sid = Atomic(Int64).new(0_i64)
    @subscriptions = {} of Int64 => Subscription
    @out = Mutex.new
    @pings = Atomic(Int32).new(0)
    getter? data_waiting = false

    def initialize(@uri = URI.parse("nats:///"))
      s = @socket = TCPSocket.new(
        uri.host.presence || "localhost",
        uri.port || 4222,
      )
      s.tcp_nodelay = true
      s.sync = false
      s.read_buffering = true
      s.buffer_size = BUFFER_SIZE

      @socket << "CONNECT "
      {
        verbose: false,
        pedantic: false,
        lang: "Crystal",
        version: VERSION,
        protocol: 1,
        # name: ???,
        # user: ???,
        # pass: ???,
      }.to_json @socket
      @socket << "\r\n"
      ping
      @socket.flush
      @socket.gets # server info
      @socket.gets # pong
      @inbox_prefix = "_INBOX.#{Random::Secure.hex}"
      @inbox_handlers = {} of String => Proc(Message, Nil)
      subscribe "#{@inbox_prefix}.*" do |msg|
        if handler = @inbox_handlers[msg.subject]?
          handler.call msg
        end
      end

      spawn begin_pings
      spawn begin_outbound
      spawn begin_inbound
    end

    def subscribe(subject : String, queue_group : String? = nil, &block : Message, Subscription ->) : Subscription
      sid = @current_sid.add 1

      @out.synchronize do
        @socket << "SUB " << subject << ' '
        if queue_group
          @socket << queue_group << ' '
        end
        @socket << sid << "\r\n"
        @data_waiting = true
      end

      @subscriptions[sid] = Subscription.new(subject, sid, &block)
    end

    def unsubscribe(subscription : Subscription) : Nil
      unsubscribe subscription.sid
    end

    def unsubscribe(subscription : Subscription, max_messages : Int) : Nil
      unsubscribe subscription.sid, max_messages
    end

    def unsubscribe(sid : Int) : Nil
      @out.synchronize do
        @socket << "UNSUB " << sid << "\r\n"
        @subscriptions.delete sid
        @data_waiting = true
      end
    end

    def unsubscribe(sid : Int, max_messages : Int) : Nil
      @out.synchronize do
        @socket << "UNSUB " << sid << ' ' << max_messages << "\r\n"
        @subscriptions[sid].unsubscribe_after messages: max_messages
        @data_waiting = true
      end
    end

    def request(subject : String, message : Data, timeout : Time::Span) : Message?
      channel = Channel(Message).new(1)
      inbox = Random::Secure.hex(4)
      key = "#{@inbox_prefix}.#{inbox}"
      @inbox_handlers[key] = ->(msg : Message) do
        channel.send msg
        @inbox_handlers.delete msg.subject
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
        @inbox_handlers.delete msg.subject
      end
      publish subject, message, reply_to: inbox

      spawn remove_key(key, after: timeout)
    end

    private def remove_key(key, after timeout)
      sleep timeout
      @inbox_handlers.delete key
    end

    def reply(msg : Message, body : Data) : Nil
      if subject = msg.reply_to
        publish subject, body
      end
    end

    def publish(subject : String, message : Data, reply_to : String? = nil) : Nil
      if message.bytesize > MAX_PUBLISH_SIZE
        raise Error.new("Attempted to publish message of size #{message.bytesize}. Cannot publish messages larger than #{MAX_PUBLISH_SIZE}.")
      end

      @out.synchronize do
        @socket << "PUB " << subject
        if reply_to
          @socket << ' ' << reply_to
        end
        @socket << ' ' << message.bytesize << "\r\n"
        @socket.write message.to_slice
        @socket << "\r\n"
        @data_waiting = true
      end
    end

    def ping
      @out.synchronize do
        @socket << "PING\r\n"
        @pings.add 1
        @data_waiting = true
      end
    end

    def pong
      @out.synchronize do
        @socket << "PONG\r\n"
        @data_waiting = true
      end
    end

    private def begin_pings
      loop do
        sleep 5.seconds
        return if @socket.closed?
        ping
      end
    end

    MAX_OUTBOUND_INTERVAL = 10.milliseconds
    @outbound_interval : Time::Span = 5.microseconds
    private def begin_outbound
      loop do
        sleep @outbound_interval
        return if @socket.closed?

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
        pp data_waiting: data_waiting?, outbound_interval: @outbound_interval
        pp outbound: ex
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
            subscription.call Message.new(subject, body, reply_to: reply_to)
            if (messages_remaining = subscription.messages_remaining) && messages_remaining <= 0
              @subscriptions.delete sid
            end
          end
        when "PING"
          pong
        when "PONG"
          @pings.sub 1
        when .starts_with? "-ERR"
          on_error line
        else
          puts "NO IDEA: #{line.inspect}"
        end
      end
    rescue ex : IO::Error
      pp ex
      close
    end

    def close
      @socket.close
    rescue IO::Error
    end

    @on_error = ->(error : Error) {}
    def on_error(&@on_error : Error ->) : Nil
    end

    private def on_error(line : String)
      @on_error.call Error.new(line)
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
    getter subject : String
    getter sid : Int64
    getter messages_remaining : Int32?

    def initialize(@subject, @sid, &@block : Message, Subscription ->)
    end

    def unsubscribe_after(messages @messages_remaining : Int32)
    end

    def call(message)
      @block.call message, self
    ensure
      if remaining = @messages_remaining
        @messages_remaining = remaining - 1
      end
    end
  end
end
