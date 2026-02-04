require "uri"
require "json"
require "socket"
require "uuid"
require "openssl"
require "log"

require "./message"
require "./headers"
require "./nuid"
require "./nkeys"
require "./version"

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
  alias Data = String | Bytes

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
    getter go : String
    getter host : String
    getter port : Int32
    getter? headers : Bool
    getter max_payload : Int64
    getter proto : Int32
    getter? auth_required : Bool = false
    getter? tls_required : Bool = false
    getter? tls_verify : Bool = false
    getter? tls_available : Bool = false
    getter client_id : UInt64?
    getter client_ip : String?
    getter nonce : String?
    getter cluster : String?
    getter domain : String?
    getter connect_urls : Array(String) { [] of String }
    @[JSON::Field(key: "ldm")]
    getter? lame_duck_mode : Bool = false
  end

  LOG = ::Log.for(self)

  # Instantiating a `NATS::Client` makes a connection to one of the given NATS
  # servers.
  class Client
    alias Data = String | Bytes

    private KILOBYTE               = 1 << 10
    private BUFFER_SIZE            = 32 * KILOBYTE
    DEFAULT_PING_INTERVAL  = 2.minutes
    DEFAULT_PING_THRESHOLD = 2

    # The current state of the client's connection
    enum State
      # The connection is currently awaiting a completed NATS connection. We
      # could be awaiting TCP, TLS, NATS protocol handshake, synchronization,
      # etc. Ideally, a client doesn't spend more than a few milliseconds in
      # this state.
      Connecting

      # A successful NATS connection has been made.
      Connected

      # The client has been disconnected and is either currently executing its
      # disconnect handler or is waiting on the reconnect backoff period.
      Disconnected

      # The disconnect handler has been invoked, the backoff period has elapsed
      # and the client is currenty attempting to reconnect to the NATS server.
      Reconnecting

      # The client has been explicitly closed with `NATS::Client#close`.
      Closed
    end

    @socket : TCPSocket | OpenSSL::SSL::Socket::Client
    @io : IO
    @current_sid = Atomic(Int64).new(0_i64)
    @subscriptions = {} of Int64 => Subscription
    @out = Mutex.new(protection: :reentrant)
    @reconnect_mutex = Mutex.new(protection: :reentrant)
    @handler_mutex = Mutex.new
    @ping_count : Atomic(Int32)
    @pings : Channel(Channel(Nil)) # For flushing the connection
    @disconnect_buffer = IO::Memory.new
    @inbox_prefix = "_INBOX.#{Random::Secure.hex}"
    @inbox_handlers = {} of String => Proc(Message, Nil)
    getter nuid = NUID.new

    # The current state of the connection
    getter state : State = :connecting
    getter server_info : ServerInfo
    private getter? data_waiting = false

    def self.new(
      *,
      ping_interval = DEFAULT_PING_INTERVAL,
      max_pings_out = DEFAULT_PING_THRESHOLD,
      nkeys_file : String? = nil,
      user_credentials : String? = nil,
    )
      new(
        servers: ENV
          .fetch("NATS_SERVERS", "nats:///")
          .split(',')
          .map { |url| URI.parse url },
        ping_interval: ping_interval,
        max_pings_out: max_pings_out,
        nkeys_file: nkeys_file,
        user_credentials: user_credentials,
      )
    end

    # Connect to a single NATS server at the given URI
    #
    # ```
    # nats = NATS::Client.new(URI.parse("nats://nats.example.com"))
    # ```
    def self.new(
      uri : URI,
      ping_interval = DEFAULT_PING_INTERVAL,
      max_pings_out = DEFAULT_PING_THRESHOLD,
      nkeys_file : String? = nil,
      user_credentials : String? = nil,
    )
      new([uri], ping_interval: ping_interval, max_pings_out: max_pings_out, nkeys_file: nkeys_file, user_credentials: user_credentials)
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
      @servers : Array(URI),
      @ping_interval : Time::Span = DEFAULT_PING_INTERVAL,
      @max_pings_out = DEFAULT_PING_THRESHOLD,
      @nkeys_file : String? = nil,
      @user_credentials : String? = nil,
    )
      uri = @servers.sample
      @ping_count = Atomic.new(0)
      @pings = Channel(Channel(Nil)).new(max_pings_out + 1) # For flushing the connection
      @inbox_handlers = {} of String => Proc(Message, Nil)

      case uri.scheme
      when "nats"
        tls = false
      when "tls"
        tls = true
      else
        raise Error.new("Unknown URI scheme #{uri.scheme.inspect}, must be tls:// or nats://")
      end
      default_port = 4222
      host = uri.host.presence || "localhost"
      port = uri.port || default_port
      LOG.trace { "Connecting to #{host}:#{port}..." }
      socket = TCPSocket.new(host, port, connect_timeout: 5.seconds)
      socket.tcp_nodelay = true
      socket.sync = false
      socket.read_buffering = true
      socket.write_timeout = 10.seconds
      socket.tcp_keepalive_count = 10
      socket.tcp_keepalive_interval = 1 # second
      socket.buffer_size = BUFFER_SIZE

      info_line = socket.read_line
      LOG.trace { "RECEIVED: #{info_line}" }
      @server_info = ServerInfo.from_json info_line[5..-1]

      if tls || @server_info.tls_required?
        context = OpenSSL::SSL::Context::Client.new
        context.add_options(
          OpenSSL::SSL::Options::ALL |       # various workarounds
          OpenSSL::SSL::Options::NO_SSL_V2 | # disable overly deprecated SSLv2
          OpenSSL::SSL::Options::NO_SSL_V3   # disable deprecated SSLv3
        )
        socket = OpenSSL::SSL::Socket::Client.new(socket, context)
        socket.sync = false
        socket.read_buffering = true
      end

      socket << "CONNECT "
      connect = {
        verbose:       false,
        pedantic:      false,
        lang:          "crystal",
        version:       VERSION,
        protocol:      1,
        headers:       true,
        no_responders: true,
        name:          uri.path.sub(%r{\A/}, "").presence,
        user:          uri.user,
        pass:          uri.password,
      }
      if @server_info.auth_required? && (nonce = @server_info.nonce)
        if user_credentials
          creds_data = File.read(user_credentials).lines
          unless jwt_begin_line = creds_data.index(&.includes?("BEGIN NATS USER JWT"))
            raise ArgumentError.new("Could not locate JWT in credentials file #{user_credentials}")
          end
          jwt = creds_data[jwt_begin_line + 1].chomp
          unless nkey_begin_line = creds_data.index(&.includes?("BEGIN USER NKEY SEED"))
            raise ArgumentError.new("Could not locate NKEYS seed in credentials file #{user_credentials}")
          end
          nkey = NKeys.new(creds_data[nkey_begin_line + 1].chomp)
          connect = connect
            .merge({jwt: jwt})
            .merge(sign_nonce(nkey, nonce))
        elsif nkeys_file
          nkey = NKeys.new(File.read(nkeys_file.strip))

          connect = connect.merge(sign_nonce(nkey, nonce))
        else
          raise Error.new("Server requires auth and supplied a nonce, but no NKEYS file specified")
        end
      end
      connect.to_json socket
      socket << "\r\n"
      socket << "PING\r\n"
      socket.flush
      until (line = socket.read_line) == "PONG"
        LOG.trace { line }
        if line.starts_with? "-ERR "
          raise Error.new(line.lchop("-ERR "))
        end
      end
      @socket = socket
      @io = socket
      if @state.reconnecting?
        subscriptions = @subscriptions
        @subscriptions = {} of Int64 => Subscription
        subscriptions.each_value do |subscription|
          LOG.trace { "Resubscribing to subscription #{subscription.subject}#{" (queue_group: #{subscription.queue_group}}" if subscription.queue_group} on subscription id #{subscription.sid}..." }
          resubscribe subscription
        end
        IO.copy @disconnect_buffer.rewind, socket
        socket.flush
        @disconnect_buffer = IO::Memory.new
      else
        spawn name: "NATS::Client#begin_pings (#{object_id})" { begin_pings }
        spawn name: "NATS::Client#begin_outbound (#{object_id})" { begin_outbound }
        spawn name: "NATS::Client#begin_inbound (#{object_id})" { begin_inbound }

        inbox_subject = "#{@inbox_prefix}.>"
        LOG.trace { "Subscribing to inbox: #{inbox_subject}" }
        subscribe inbox_subject do |msg|
          if handler = @inbox_handlers[msg.subject]?
            handler.call msg
          end
        end
      end

      @state = :connected

      # NO DIRECT SOCKET READS PAST THIS POINT
      # ALL SOCKET READS MUST BE DONE IN #begin_inbound PAST THIS POINT
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
    #   order = Order.from_json(String.new(msg.body))
    #
    #   # ...
    # end
    # ```
    def subscribe(subject : String, queue_group : String? = nil, sid = @current_sid.add(1), max_in_flight = 64_000, *, concurrency = 1, &block : Message, Subscription ->) : Subscription
      validate_subscribe_subject! subject

      LOG.trace { "Subscribing to #{subject.inspect}, queue_group: #{queue_group.inspect}, sid: #{sid}" }
      write do
        @io << "SUB " << subject << ' '
        if queue_group
          @io << queue_group << ' '
        end
        @io << sid << "\r\n"
      end

      @subscriptions[sid] = Subscription.new(subject, sid, queue_group, self, max_in_flight: max_in_flight, concurrency: concurrency, &block).tap(&.start)
    end

    private def resubscribe(subscription : Subscription)
      subscribe subscription.subject, subscription.queue_group, subscription.sid,
        max_in_flight: subscription.max_in_flight,
        concurrency: subscription.concurrency,
        &subscription.@block
    end

    private def sign_nonce(nkey, nonce)
      {
        sig:  Base64.urlsafe_encode(nkey.keypair.sign(nonce), padding: false),
        nkey: Base32.encode(nkey.keypair.public_key, pad: false),
      }
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

    private def unsubscribe(sid : Int) : Nil
      LOG.trace { "Unsubscribing from sid: #{sid}" }
      write { @io << "UNSUB " << sid << "\r\n" }
    ensure
      if subscription = @subscriptions.delete sid
        subscription.close
      end
    end

    private def unsubscribe(sid : Int, max_messages : Int) : Nil
      LOG.trace { "Unsubscribing from sid #{sid} after #{max_messages} messages" }
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
    #   response << Order.from_json(String.new(order_response.body))
    # else
    #   response.status = :service_unavailable
    # end
    # ```
    def request(subject : String, message : Data = "", timeout : Time::Span = 2.seconds, headers : Headers? = nil, *, flush = true) : Message?
      channel = Channel(Message).new(1)
      inbox = @nuid.next
      key = "#{@inbox_prefix}.#{inbox}"
      @handler_mutex.synchronize do
        @inbox_handlers[key] = ->(msg : Message) do
          channel.send msg unless channel.closed?
        end
      end
      publish subject, message, reply_to: key, headers: headers

      # TODO: Track how often we're making requests. If we're making requests
      # often enough, we don't need to flush the buffer after every request, and
      # can instead rely on the buffer being flushed as a result of the sheer
      # volume of data going through it. For example, if the socket buffer is
      # 8KB and we send more than 8KB every millisecond, this mutex lock and
      # socket flush are unnecessary and we could potentially coalesce multiple
      # requests within the same socket flush, reducing the number of syscalls.
      flush! if flush

      begin
        select
        when msg = channel.receive?
          if msg
            unless msg.body.empty? && msg.headers.try(&.["Status"]?) == "503"
              msg
            end
          end
        when timeout(timeout)
          channel.close
          nil
        end
      ensure
        @inbox_handlers.delete key
      end
    end

    # Make a synchronous request to subscribers of the given `subject`, waiting
    # up to `timeout` for responses from any of the subscribers. The first
    # `max_replies` messages to come back will be returned. If fewer replies are
    # received before the `timeout` elapses, only those will be returned.
    #
    # ```
    # orders = nats.request_many("orders.info.#{order_id}", max_replies: 10).map do |response|
    #   Order.from_json(response.data_string)
    # end
    # ```
    def request_many(subject : String, message : Data = "", timeout : Time::Span = 2.seconds, headers : Headers? = nil, *, max_replies : Int32, flush = true) : Array(Message)
      if max_replies.negative?
        raise ArgumentError.new("max_replies must not be negative")
      end

      replies = Array(Message).new(max_replies)
      channel = Channel(Message).new(max_replies)
      inbox = @nuid.next
      key = "#{@inbox_prefix}.#{inbox}"
      @handler_mutex.synchronize do
        @inbox_handlers[key] = ->(msg : Message) do
          channel.send msg unless channel.closed?
        end
      end
      publish subject, message, reply_to: key, headers: headers
      original_timeout = timeout

      flush! if flush

      start = Time.monotonic
      begin
        loop do
          select
          when msg = channel.receive?
            if msg
              unless msg.body.empty? && msg.headers.try(&.["Status"]?) == "503"
                replies << msg
              end
            end
            if replies.size >= max_replies || Time.monotonic - start >= original_timeout
              return replies
            end
            timeout = original_timeout - (Time.monotonic - start)
          when timeout(timeout)
            channel.close
            return replies
          end
        end
      ensure
        @inbox_handlers.delete key
      end
    end

    def request_many(messages : Enumerable(Message), timeout : Time::Span = 2.seconds, flush = true)
      channel = Channel({Message, Int32}).new(messages.size)
      replies = Array(Message?).new(messages.size) { nil }
      inbox_prefix = "#{@inbox_prefix}.#{UUID.v7}"
      handler_keys = [] of String
      begin
        @handler_mutex.synchronize do
          messages.each_with_index do |message, index|
            reply_subject = "#{inbox_prefix}.#{index}"
            handler_keys << reply_subject

            @inbox_handlers[reply_subject] = Proc(Message, Nil).new do |msg|
              # mutex.synchronize { replies[index] = msg }
              channel.send({msg, index})
            end
            message.reply_to = reply_subject
            publish message
          end
        end
        flush! if flush

        start = Time.monotonic
        messages.size.times do
          elapsed = Time.monotonic - start
          remaining = timeout - elapsed

          select
          when reply = channel.receive
            msg, index = reply
            replies[index] = msg
          when timeout(timeout)
            break
          end
        end

        replies
      ensure
        @handler_mutex.synchronize do
          handler_keys.each do |key|
            @inbox_handlers.delete key
          end
        end
      end
    end

    # Make a synchronous request to subscribers of the given `subject`,
    # receiving as many messages as are sent until `stall_timeout` has passed
    # without receiving any messages. If no replies are received before the
    # `stall_timeout` elapses, the return value will be empty.
    #
    # ```
    # orders = nats.request("orders.info.#{order_id}", stall_timeout: 2.seconds).map do |response|
    #   Order.from_json(response.data_string)
    # end
    # ```
    def request_many(subject : String, message : Data = "", headers : Headers? = nil, *, stall_timeout : Time::Span = 2.seconds, flush = true) : Array(Message)
      if stall_timeout.negative?
        raise ArgumentError.new("stall_timeout must not be negative")
      end
      replies = Array(Message).new
      channel = Channel(Message).new(1)
      inbox = @nuid.next
      key = "#{@inbox_prefix}.#{inbox}"
      @handler_mutex.synchronize do
        @inbox_handlers[key] = ->(msg : Message) do
          channel.send msg unless channel.closed?
        end
      end
      publish subject, message, reply_to: key, headers: headers

      flush! if flush

      start = Time.monotonic
      begin
        loop do
          select
          when msg = channel.receive?
            if msg
              unless msg.body.empty? && msg.headers.try(&.["Status"]?) == "503"
                replies << msg
              end
            end
          when timeout(stall_timeout)
            channel.close
            return replies
          end
        end
      ensure
        @handler_mutex.synchronize do
          @inbox_handlers.delete key
        end
      end
    end

    # Make a synchronous request to subscribers of the given `subject`, waiting
    # up to `timeout` for responses from any of the subscribers and yielding
    # each message to the given block to check for a sentinel message. The first
    # message that returns `true` for the block will terminate the request and
    # return all messages received up to that point.
    #
    # ```
    # orders = nats.request_many("orders.info.#{order_id}") do |response|
    #   # Our sentinel message will be empty
    #   response.data.empty?
    # end
    # ```
    def request_many(subject : String, message : Data = "", timeout : Time::Span = 2.seconds, headers : Headers? = nil, *, flush = true, &) : Array(Message)
      replies = Array(Message).new
      channel = Channel(Message).new(10)
      inbox = @nuid.next
      key = "#{@inbox_prefix}.#{inbox}"
      @handler_mutex.synchronize do
        @inbox_handlers[key] = ->(msg : Message) do
          channel.send msg unless channel.closed?
        end
      end
      publish subject, message, reply_to: key, headers: headers
      original_timeout = timeout

      flush! if flush

      start = Time.monotonic
      begin
        loop do
          select
          when msg = channel.receive?
            if msg
              unless msg.body.empty? && msg.headers.try(&.["Status"]?) == "503"
                if yield msg
                  return replies
                end
                replies << msg
              end
            end
            if Time.monotonic - start >= original_timeout
              return replies
            end
            timeout = original_timeout - (Time.monotonic - start)
          when timeout(timeout)
            channel.close
            return replies
          end
        end
      ensure
        @handler_mutex.synchronize do
          @inbox_handlers.delete key
        end
      end
    end

    # Make an asynchronous request to subscribers of the given `subject`, not
    # waiting for a response. The first message to come back will be passed to
    # the block.
    def request(subject : String, message : Data = "", timeout = 2.seconds, &block : Message ->) : Nil
      inbox = @nuid.next
      key = "#{@inbox_prefix}.#{inbox}"
      handler = ->(msg : Message) do
        block.call msg
        remove_key key
      end
      @handler_mutex.synchronize { @inbox_handlers[key] = handler }
      publish subject, message, reply_to: key

      spawn remove_key(key, after: timeout)
    end

    private def remove_key(key, after timeout = nil)
      sleep timeout if timeout
      @handler_mutex.synchronize { @inbox_handlers.delete key }
    end

    # Send the given `body` to the `msg`'s `reply_to` subject, often used in a
    # request/reply messaging model.
    #
    # ```
    # nats.subscribe "orders.*", queue_group: "orders-service" do |msg|
    #   _, id = msg.subject.split('.') # Similar to HTTP path routing
    #
    #   if order = OrderQuery.new.find_by(id: id)
    #     nats.reply msg, {order: order}.to_json
    #   else
    #     nats.reply msg, {error: "No order with that id found"}.to_json
    #   end
    # end
    # ```
    def reply(msg : Message, body : Data = "", headers : Headers? = nil, *, flush = true) : Nil
      if subject = msg.reply_to
        publish subject, body, headers: headers
        flush! if flush
      else
        raise NotAReply.new("Cannot reply to a message that has no return address", msg)
      end
    end

    protected def publish(msg : Message)
      publish msg.subject, msg.data, reply_to: msg.reply_to, headers: msg.headers
    end

    # Publish the given message body (either `Bytes` for binary data or `String` for text) on the given NATS subject, optionally supplying a `reply_to` subject (if expecting a reply or to notify the receiver where to send updates) and any `headers`.
    #
    # ```
    # # Send an empty message to a subject
    # nats.publish "hello"
    #
    # # Serialize an object to a subject
    # nats.publish "orders.#{order.id}", order.to_json
    #
    # # Tell a recipient where to send results. For example, to stream results
    # # to a given subject:
    # reply_subject = "replies.orders.list.customer.123"
    # orders = [] of Order
    # nats.subscribe reply_subject do |msg|
    #   case result = (Order | Complete).from_json(String.new(msg.body))
    #   in Order
    #     orders << result
    #   in Complete
    #     nats.unsubscribe reply_subject
    #   end
    # end
    # nats.publish "orders.list.customer.123", reply_to: reply_subject
    #
    # # Publish a message to NATS JetStream with a message-deduplication header
    # # for idempotency:
    # nats.jetstream.subscribe consumer_subject, queue_group: "my-service" do |msg|
    #   # ...
    # end
    # nats.publish orders_subject, order.to_json, headers: NATS::Message::Headers{
    #   # Deduplicate using the equivalent of a cache key
    #   "Nats-Msg-Id" => "order-submitted-#{order.id}-#{order.updated_at.to_json}",
    # }
    # ```
    def publish(subject : String, message : Data = Bytes.empty, reply_to : String? = nil, headers : Message::Headers? = nil) : Nil
      validate_publish_subject! subject

      if subject.includes? ' '
        raise ArgumentError.new("Cannot publish to a subject that contains a space")
      end
      if message.bytesize > @server_info.max_payload
        raise Error.new("Attempted to publish message of size #{message.bytesize}. Cannot publish messages larger than #{@server_info.max_payload}.")
      end

      LOG.debug { "Publishing #{message.bytesize} bytes to #{subject.inspect}, reply_to: #{reply_to.inspect}, headers: #{headers.inspect}" }
      write do
        if headers
          @io << "HPUB "
        else
          @io << "PUB "
        end

        @io << subject
        if reply_to
          @io << ' ' << reply_to
        end

        if headers
          nats_header_preamble = "NATS/1.0\r\n"
          initial_header_length = nats_header_preamble.bytesize + 2 # 2 extra bytes for final CR+LF
          header_length = headers.reduce(initial_header_length) do |bytes, (key, values)|
            values.each do |value|
              bytes += key.bytesize + value.bytesize + 4 # 2 extra bytes for ": " and 2 for CR+LF
            end
            bytes
          end
          @io << ' ' << header_length
          @io << ' ' << header_length + message.bytesize << "\r\n"
          @io << nats_header_preamble
          headers.each do |key, values|
            values.each do |value|
              @io << key << ": " << value << "\r\n"
            end
          end
          @io << "\r\n"
        else
          @io << ' ' << message.bytesize << "\r\n"
        end

        @io.write message.to_slice
        @io << "\r\n"
      end
    end

    def drain
      flush

      # Snapshot the list of subscriptions because we're about to remove them
      # from the hash
      subscriptions = @subscriptions.values

      # Make sure we don't get any new messages
      @subscriptions.each { |sid, _| unsubscribe sid }
      flush

      # Run through the snapshot of subscriptions we took above and wait until
      # we process all of the messages in memory.
      subscriptions.each(&.drain)
    end

    # Flush the client's output buffer over the wire
    def flush(timeout = 2.seconds)
      channel = Channel(Nil).new(1)
      ping channel
      LOG.trace { "Flushing buffer..." }
      flush!

      Fiber.yield

      select
      when channel.receive
      when timeout(timeout)
        raise Error.new("Flush did not complete within duration: #{timeout}")
      end
    end

    def flush!
      @out.synchronize do
        @socket.flush
        @data_waiting = false
      end
    end

    def ping(channel = Channel(Nil).new(1))
      LOG.trace { "Sending PING" }
      write do
        @io << "PING\r\n"
        @ping_count.add 1
        @pings.send channel
      end
      @on_ping.call
    end

    # :nodoc:
    def pong
      LOG.trace { "Sending PONG" }
      write { @io << "PONG\r\n" }
      @on_pong.call
    end

    private def begin_pings
      loop do
        sleep @ping_interval
        return if @state.closed?
        if @ping_count.get > @max_pings_out
          LOG.warn { "Too many unresolved pings. Reconnecting..." }
          handle_disconnect!
        end
        ping
      end
    end

    private OUTBOUND_INTERVAL = ENV
      .fetch("NATS_FLUSH_INTERVAL_MS", "10")
      .to_i { 10 } # if we can't parse an int, just use 10
      .milliseconds

    private def begin_outbound
      loop do
        sleep OUTBOUND_INTERVAL
        return if state.closed?

        if data_waiting?
          LOG.trace { "Flushing output buffer..." }
          flush!
          LOG.trace { "Output flushed." }
        end
      rescue ex : IO::Error
        break if state.closed?
      rescue ex
        @on_error.call ex
      end
    end

    private def begin_inbound
      backoff = 1.millisecond
      loop do
        break if @socket.closed? && state.closed?
        unless state.connected?
          Fiber.yield
          next
        end

        line = @socket.read_line
        break if state.closed?
        LOG.trace { line || "" }
        case line
        when .starts_with?("MSG"), .starts_with?("HMSG")
          starting_point = 4 # "MSG "
          has_headers = line.starts_with?('H')
          starting_point += 1 if has_headers

          if (subject_end = line.index(' ', starting_point)) && (sid_end = line.index(' ', subject_end + 1))
            subject = line[starting_point...subject_end]
            sid = line[subject_end + 1...sid_end].to_i

            # Figure out if we got a reply_to and set it and bytesize accordingly
            reply_to_with_byte_size = line[sid_end + 1..-1]
            if has_headers # HMSG
              # An HMSG event from the server looks like this (brackets imply optional):
              #   HMSG my-subject my-sid [my-reply-to] header_size total_size
              #   NATS/1.0
              #   My-Key: My-Value
              #
              #   My Payload Goes Here
              #
              # Total size includes header size, so payload_size = total_size - header_size
              if reply_to_boundary = reply_to_with_byte_size.index(' ')
                # 3 tokens: REPLY_TO HEADER_SIZE TOTAL_SIZE
                if header_length_boundary = reply_to_with_byte_size.index(' ', reply_to_boundary + 1)
                  reply_to = reply_to_with_byte_size[0...reply_to_boundary]
                  header_size = reply_to_with_byte_size[reply_to_boundary + 1...header_length_boundary].to_i
                  bytesize = reply_to_with_byte_size[header_length_boundary + 1..-1].to_i - header_size
                else # Only 2 tokens: HEADER_SIZE TOTAL_SIZE
                  header_size = reply_to_with_byte_size[0...reply_to_boundary].to_i
                  bytesize = reply_to_with_byte_size[reply_to_boundary + 1..-1].to_i - header_size
                end
              else
                raise Error.new("Invalid message declaration with headers: #{line}")
              end
              headers = Message::Headers.new
              # Headers preamble, intended to look like HTTP/1.1
              if (header_decl = @socket.read_line).starts_with? "NATS/1.0"
                # If there is anything beyond the NATS/1.0  status line, that
                # indicates the request stauts and becomes the status header of
                # the reply message.
                if header_decl.size > "NATS/1.0 ".size
                  headers["Status"] = header_decl["NATS/1.0 ".size..]
                end
                until (header_line = @socket.read_line).empty?
                  key, value = header_line.split(/:\s*/, 2)
                  headers.add key, value
                end
                LOG.trace { "Headers: #{headers.inspect}" }
              else
                raise Error.new("Invalid header declaration: #{header_decl} (msg: #{line})")
              end
            else # MSG
              if boundary = reply_to_with_byte_size.rindex(' ')
                reply_to = reply_to_with_byte_size[0...boundary]
                bytesize = reply_to_with_byte_size[boundary + 1..-1].to_i
              else
                bytesize = reply_to_with_byte_size.to_i
              end
            end
          else
            raise Error.new("Invalid message declaration: #{line.inspect}")
          end

          body = @socket.read_string(bytesize)
          @socket.skip 2 # CRLF

          if subscription = @subscriptions[sid]?
            subscription.send Message.new(subject, body, reply_to: reply_to, headers: headers) do |ex|
              LOG.trace { "Error occurred in handling subscription #{sid}: #{ex}" }
              @on_error.call ex
            end
            if subscription.messages_remaining
              LOG.trace { "Messages remaining in subscription #{sid} to #{subscription.subject}: #{subscription.messages_remaining}" }
            end
            if (messages_remaining = subscription.messages_remaining) && messages_remaining <= 0
              @subscriptions.delete sid
              subscription.close
            end
          else
            LOG.debug { "No subscription #{sid}" }
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
        when .starts_with? "INFO"
          @server_info = ServerInfo.from_json line[5..-1]
          if (urls = @server_info.connect_urls).any?
            @servers = urls.map do |host_with_port| # it's not a real URL :-(
              URI.parse("nats://#{host_with_port}")
            end
          end
        when .starts_with? "-ERR"
          @on_error.call Error.new(line)
        else
          @on_error.call UnknownCommand.new(line)
        end
        backoff = 1.millisecond
        Fiber.yield
      rescue ex : IO::Error
        return if state.closed?
        backoff = {backoff * 2, 10.seconds}.min
        handle_inbound_disconnect ex, backoff: backoff
      end
    ensure
      LOG.warn { "Exited inbound message loop" }
    end

    private def handle_inbound_disconnect(exception, backoff : Time::Span)
      LOG.warn(exception: exception) { "Exception in inbound data handler" }
      handle_disconnect! backoff
    end

    # Close this NATS connection. This should be done explicitly before exiting
    # the program so that the NATS server can remove any subscriptions that were
    # associated with this client.
    def close : Nil
      return if @state.closed?
      LOG.trace { "Flushing/draining before closing..." }
      flush
      drain
    rescue IO::Error
    ensure
      # If we weren't able to set it above, we close now.
      @state = :closed
      @socket.close rescue nil
      LOG.trace { "Connection closed" }
    end

    @on_error = ->(error : Exception) { }

    # Execute the given block whenever an exception is raised inside this NATS
    # client.
    #
    # ```
    # nats = NATS::Client.new
    # nats.on_error { |error| Honeybadger.notify error }
    # ```
    def on_error(&@on_error : Exception -> Nil)
      self
    end

    @on_disconnect = -> { }

    # Execute the given block whenever this client is disconnected from the NATS
    # server.
    #
    # ```
    # nats = NATS::Client.new
    # nats.on_disconnect { Datadog.metrics.increment "nats.disconnect" }
    # ```
    def on_disconnect(&@on_disconnect)
      self
    end

    @on_reconnect = -> { }

    # Execute the given block whenever this client is reconnected to the NATS
    # server.
    #
    # ```
    # nats = NATS::Client.new
    # nats.on_reconnect { Datadog.metrics.increment "nats.reconnect" }
    # ```
    def on_reconnect(&@on_reconnect)
      self
    end

    @on_ping = -> { }

    # Execute the given block whenever this client pings the server.
    #
    # ```
    # nats = NATS::Client.new
    # nats.on_ping { Datadog.metrics.increment "nats.ping" }
    # ```
    def on_ping(&@on_ping)
      self
    end

    @on_pong = -> { }

    # Execute the given block whenever this client receives a pong reply from
    # the server.
    #
    # ```
    # nats = NATS::Client.new
    # nats.on_pong { Datadog.metrics.increment "nats.pong" }
    # ```
    def on_pong(&@on_pong)
      self
    end

    private def validate_publish_subject!(subject : String)
      unless valid_publish_subject? subject
        raise ArgumentError.new("Invalid subject: #{subject.inspect}. NATS subjects may not contain spaces, `.`, `*`, `>`, or null bytes.")
      end
    end

    private def validate_subscribe_subject!(subject : String)
      unless valid_subscribe_subject? subject
        raise ArgumentError.new("Invalid subject: #{subject.inspect}. NATS subjects may not contain spaces, `.`, `*`, `>`, or null bytes.")
      end
    end

    private INVALID_SUBJECT_CHARS = {
      '\0',
      '\n',
      ' ',
      '*',
      '>',
    }

    private INVALID_SUBSCRIBE_SUBJECT_CHARS = {'\0', ' ', '\n'}

    def valid_publish_subject?(subject : String) : Bool
      subject.each_char do |char|
        return false if char.in?(INVALID_SUBJECT_CHARS)
      end

      true
    end

    def valid_subscribe_subject?(subject : String) : Bool
      subject.each_char do |char|
        return false if char.in?(INVALID_SUBSCRIBE_SUBJECT_CHARS)
      end

      true
    end

    private def write(&) : Nil
      if @socket.closed?
        handle_disconnect!
      end
      if @state.closed?
        raise ClientClosed.new("Client has been explicitly closed.")
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

    private def handle_disconnect!(backoff : Time::Span = 1.millisecond)
      @reconnect_mutex.synchronize do
        @on_disconnect.call

        loop do
          @out.synchronize do
            unless @state.closed? || @state.connecting?
              @socket.close unless @socket.closed?
              @state = :disconnected
              # Redirect all writes to the buffer until we reconnect to the server
              @io = @disconnect_buffer
              LOG.trace { "Output set to in-memory buffer pending reconnection" }
              sleep backoff
              backoff = {backoff * 2, 10.seconds}.min
              reconnect!
            end
          end

          return
        rescue ex
          spawn @on_error.call(ex)
        end
      end
    end

    private def reconnect!
      return unless @state.disconnected?
      @state = :reconnecting
      LOG.warn &.emit "Reconnecting", servers: @servers.map(&.to_s)

      initialize(
        servers: @servers,
        ping_interval: @ping_interval,
        max_pings_out: @max_pings_out,
        nkeys_file: @nkeys_file,
        user_credentials: @user_credentials,
      )
      @on_reconnect.call
    end

    # Raised when an attempt is made to communicate with the NATS server using
    # a client instance that has been explicitly closed.
    class ClientClosed < Error
    end
  end

  class Subscription
    alias MessageChannel = Channel({Message, (Exception -> Nil)})

    getter subject : String
    getter sid : Int64
    getter queue_group : String?
    getter messages_remaining : Int32?
    getter concurrency : Int32
    getter max_in_flight : Int32
    getter? closed = false
    private getter nats : Client
    private getter message_channel : MessageChannel
    private getter? processing = false

    def initialize(@subject, @sid, @queue_group, @nats, @max_in_flight, *, @concurrency, &@block : Message, Subscription ->)
      @message_channel = MessageChannel.new(max_in_flight)
    end

    # This channel subclass doesn't eagerly allocate huge amounts of memory for
    # messages to be passed through. Subscriptions default to 64k elements
    # which would result in a huge amount of memory allocated when you have a lot of
    # NATS subscriptions if we were using the stdlib Channel type.
    private class Channel(T) < ::Channel(T)
      def initialize(@capacity = 0)
        @closed = false

        @senders = Crystal::PointerLinkedList(Sender(T)).new
        @receivers = Crystal::PointerLinkedList(Receiver(T)).new

        @queue = Deque(T).new
      end

      def queue
        @queue.not_nil!
      end
    end

    def unsubscribe_after(messages @messages_remaining : Int32)
    end

    def start : self
      concurrency.times do
        spawn do
          remaining = @messages_remaining
          while remaining.nil? || remaining > 0
            if result = message_channel.receive?
              message, on_error = result
              @processing = true

              LOG.trace { "Calling subscription handler for sid #{sid} (subscription to #{subject.inspect}, message subject #{message.subject.inspect})" }
              begin
                call message, on_error
              ensure
                @processing = false
              end

              remaining = @messages_remaining
            else
              break
            end
          end
        rescue ex
        end
      end

      self
    end

    def drain : Nil
      until @message_channel.queue.empty? && !processing?
        sleep 1.millisecond
      end
    end

    def close
      @nats.unsubscribe self
      @message_channel.close
      @closed = true
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
