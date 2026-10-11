require "uri"
require "json"
require "socket"
require "uuid"
require "openssl"

require "./error"
require "./message"
require "./event_parser"
require "./server_info"
require "./headers"
require "./nuid"
require "./nkeys"
require "./version"
require "./log"

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

  # Raised when trying to reply to a NATS message that is not a reply.
  class NotAReply < Error
    getter nats_message : Message

    def initialize(error_message, @nats_message : Message)
      super error_message
    end
  end

  class ServerNotRespondingToPings < Error
  end

  # Instantiating a `NATS::Client` makes a connection to one of the given NATS
  # servers.
  class Client
    alias Data = String | Bytes

    private KILOBYTE              = 1 << 10
    private BUFFER_SIZE           = 32 * KILOBYTE
    private MIN_RECONNECT_BACKOFF = 1.millisecond
    private MAX_RECONNECT_BACKOFF = 10.seconds
    # A connection that survives at least this long is considered stable, so
    # the next disconnect starts the reconnect backoff over from the minimum.
    # Connections that die faster than this keep growing the backoff so that a
    # server that accepts connections and immediately drops them doesn't get
    # hammered.
    private STABLE_CONNECTION_DURATION = 5.seconds
    # How long to wait for TCP/TLS connection and the NATS handshake
    private CONNECT_TIMEOUT        = 5.seconds
    DEFAULT_PING_INTERVAL  = 2.minutes
    DEFAULT_PING_THRESHOLD = 2
    # How much outgoing data we will hold in memory while disconnected from the
    # server. Once this is exceeded, publishing raises `ReconnectBufferExceeded`
    # until the connection is re-established.
    DEFAULT_RECONNECT_BUFFER_SIZE = 8 * KILOBYTE * KILOBYTE

    # Raised when publishing while disconnected from the server and the amount
    # of data buffered in memory awaiting reconnection has exceeded the
    # `reconnect_buffer_size` given to the client.
    class ReconnectBufferExceeded < Error
    end

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
    # Where outgoing protocol data is written. This is the socket while we are
    # connected and `@disconnect_buffer` while we are not.
    @io : IO
    @current_sid = Atomic(Int64).new(0_i64)
    @subscriptions = {} of Int64 => Subscription
    @out = Mutex.new(protection: :reentrant)
    @handler_mutex = Mutex.new(protection: :reentrant)
    # Number of PINGs we have sent that have not yet received a PONG
    @ping_count = Atomic(Int32).new(0)
    # Channels waiting on PONGs, in the order their PINGs were sent, used for
    # flushing the connection.
    @pings = Deque(Channel(Nil)).new
    @pings_mutex = Mutex.new
    @disconnect_buffer = IO::Memory.new
    @reconnect_backoff = MIN_RECONNECT_BACKOFF
    @reconnect_buffer_size : Int64
    @connected_at : Time::Instant?
    @inbox_prefix = "_INBOX.#{Random::Secure.hex}"
    @inbox_handlers = {} of String => Proc(Message, Nil)
    # Servers the cluster told us about via `connect_urls`. These are tried
    # alongside the configured `@servers`, which we never overwrite so that
    # they remain available as a fallback.
    @discovered_servers = [] of URI
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
      reconnect_buffer_size : Int = DEFAULT_RECONNECT_BUFFER_SIZE,
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
        reconnect_buffer_size: reconnect_buffer_size,
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
      reconnect_buffer_size : Int = DEFAULT_RECONNECT_BUFFER_SIZE,
    )
      new([uri], ping_interval: ping_interval, max_pings_out: max_pings_out, nkeys_file: nkeys_file, user_credentials: user_credentials, reconnect_buffer_size: reconnect_buffer_size)
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
      reconnect_buffer_size : Int = DEFAULT_RECONNECT_BUFFER_SIZE,
    )
      @reconnect_buffer_size = reconnect_buffer_size.to_i64
      @inbox_handlers = {} of String => Proc(Message, Nil)

      socket, @server_info = connect
      @socket = socket
      @io = socket
      @state = :connected
      @connected_at = Time.instant

      # NO DIRECT SOCKET READS PAST THIS POINT
      # ALL SOCKET READS MUST BE DONE IN #begin_inbound PAST THIS POINT

      spawn name: "NATS::Client#begin_pings (#{object_id})" { begin_pings }
      spawn name: "NATS::Client#begin_outbound (#{object_id})" { begin_outbound }
      spawn name: "NATS::Client#begin_inbound (#{object_id})" { begin_inbound }

      inbox_subject = "#{@inbox_prefix}.>"
      LOG.trace { "Subscribing to inbox: #{inbox_subject}" }
      subscribe inbox_subject do |msg|
        if handler = get_handler(msg.subject)
          handler.call msg
        end
      end
    end

    # Open a TCP (and, if required, TLS) connection to one of the configured
    # servers and complete the NATS handshake on it. This intentionally does
    # not touch any of the client's connection state so that it can run while
    # other fibers keep writing into the disconnect buffer. The caller is
    # responsible for installing the returned socket.
    private def connect : {TCPSocket | OpenSSL::SSL::Socket::Client, ServerInfo}
      uri = candidate_servers.sample

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
      tcp_socket = TCPSocket.new(host, port, connect_timeout: CONNECT_TIMEOUT)
      socket = tcp_socket.as(TCPSocket | OpenSSL::SSL::Socket::Client)
      tcp_socket.tcp_nodelay = true
      tcp_socket.sync = false
      tcp_socket.read_buffering = true
      tcp_socket.write_timeout = 10.seconds
      tcp_socket.tcp_keepalive_count = 10
      tcp_socket.tcp_keepalive_interval = 1 # second
      tcp_socket.buffer_size = BUFFER_SIZE
      # A server (or load balancer) that accepts the TCP connection but never
      # completes the handshake must not hang us forever. This is cleared once
      # the handshake completes.
      tcp_socket.read_timeout = CONNECT_TIMEOUT

      begin
        info_line = socket.read_line
        LOG.trace { "RECEIVED: #{info_line}" }
        unless info_line.starts_with? "INFO "
          raise Error.new("Expected INFO from server, received: #{info_line.inspect}")
        end
        server_info = ServerInfo.from_json info_line[5..-1]

        if tls || server_info.tls_required?
          context = OpenSSL::SSL::Context::Client.new
          context.add_options(
            OpenSSL::SSL::Options::ALL |       # various workarounds
            OpenSSL::SSL::Options::NO_SSL_V2 | # disable overly deprecated SSLv2
            OpenSSL::SSL::Options::NO_SSL_V3   # disable deprecated SSLv3
          )
          socket = OpenSSL::SSL::Socket::Client.new(tcp_socket, context)
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
        if server_info.auth_required? && (nonce = server_info.nonce)
          if user_credentials = @user_credentials
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
          elsif nkeys_file = @nkeys_file
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
        tcp_socket.read_timeout = nil
      rescue ex
        socket.close rescue nil
        raise ex
      end

      {socket, server_info}
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
      subscription = Subscription.new(subject, sid, queue_group, self, max_in_flight: max_in_flight, concurrency: concurrency, &block)
      @out.synchronize do
        # While disconnected there is no point in buffering the SUB: every
        # subscription in the table is re-sent when we reconnect.
        write { write_subscription subscription, to: @io } unless buffering?
        @subscriptions[sid] = subscription
      end
      subscription.start
    end

    # Write the protocol commands needed to establish the given subscription
    # (and, if it has a message limit, that limit) on the given IO.
    private def write_subscription(subscription : Subscription, to io : IO) : Nil
      if (remaining = subscription.messages_remaining) && remaining <= 0
        return
      end

      io << "SUB " << subscription.subject << ' '
      if queue_group = subscription.queue_group
        io << queue_group << ' '
      end
      io << subscription.sid << "\r\n"

      if remaining
        io << "UNSUB " << subscription.sid << ' ' << remaining << "\r\n"
      end
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
      @out.synchronize do
        # While disconnected, the server has already forgotten about this
        # subscription and removing it from the table means we won't re-send it
        # on reconnect, so there is nothing to write. We also skip the write if
        # we don't know about the subscription, which avoids sending a second
        # UNSUB when `Subscription#close` calls back into this method.
        if @subscriptions.has_key?(sid) && !buffering?
          write { @io << "UNSUB " << sid << "\r\n" }
        end
      end
    ensure
      if subscription = @subscriptions.delete sid
        subscription.close
      end
    end

    private def unsubscribe(sid : Int, max_messages : Int) : Nil
      LOG.trace { "Unsubscribing from sid #{sid} after #{max_messages} messages" }
      @out.synchronize do
        # While disconnected, the limit is sent along with the SUB on reconnect
        write { @io << "UNSUB " << sid << ' ' << max_messages << "\r\n" } unless buffering?
      end
    ensure
      @subscriptions[sid]?.try &.unsubscribe_after messages: max_messages
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
      add_handler key do |msg|
        channel.send msg unless channel.closed?
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
        remove_handler key
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
    @[Experimental]
    def request_many(subject : String, message : Data = "", timeout : Time::Span = 2.seconds, headers : Headers? = nil, *, max_replies : Int32, flush = true) : Array(Message)
      if max_replies.negative?
        raise ArgumentError.new("max_replies must not be negative")
      end

      replies = Array(Message).new(max_replies)
      channel = Channel(Message).new(3)
      inbox = @nuid.next
      key = "#{@inbox_prefix}.#{inbox}"
      add_handler key do |msg|
        channel.send msg unless channel.closed?
      end
      publish subject, message, reply_to: key, headers: headers
      original_timeout = timeout

      flush! if flush

      start = Time.instant
      begin
        loop do
          select
          when msg = channel.receive?
            if msg
              unless msg.body.empty? && msg.headers.try(&.["Status"]?) == "503"
                replies << msg
              end
            end
            if replies.size >= max_replies || start.elapsed >= original_timeout
              return replies
            end
            timeout = original_timeout - start.elapsed
          when timeout(timeout)
            channel.close
            return replies
          end
        end
      ensure
        remove_handler key
      end
    end

    # Send all `messages` and wait up to `timeout` for all of them to return.
    # The method returns the responses for each request message in the original
    # order. Requests that received no reply before `timeout` elapsed will have
    # a `nil` response.
    @[Experimental]
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

            add_handler reply_subject do |msg|
              channel.send({msg, index})
            end
            message.reply_to = reply_subject
            publish message
          end
        end
        flush! if flush

        start = Time.instant
        messages.size.times do
          remaining = timeout - start.elapsed

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
            remove_handler key
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
    @[Experimental]
    def request_many(subject : String, message : Data = "", headers : Headers? = nil, *, stall_timeout : Time::Span = 2.seconds, flush = true) : Array(Message)
      if stall_timeout.negative?
        raise ArgumentError.new("stall_timeout must not be negative")
      end
      replies = Array(Message).new
      channel = Channel(Message).new(1)
      inbox = @nuid.next
      key = "#{@inbox_prefix}.#{inbox}"
      add_handler key do |msg|
        channel.send msg unless channel.closed?
      end
      publish subject, message, reply_to: key, headers: headers
      flush! if flush

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
        remove_handler key
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
    #
    # NOTE: The sentinel message is not included in the array of messages
    # returned by the method. It doesn't count as a reply. You should always
    # send all of your replies and then send an *additional* sentinel message.
    @[Experimental]
    def request_many(subject : String, message : Data = "", timeout : Time::Span = 2.seconds, headers : Headers? = nil, *, flush = true, &) : Nil
      channel = Channel(Message).new(10)
      inbox = @nuid.next
      key = "#{@inbox_prefix}.#{inbox}"
      add_handler key do |msg|
        channel.send msg unless channel.closed?
      end
      publish subject, message, reply_to: key, headers: headers
      original_timeout = timeout

      flush! if flush

      start = Time.instant
      begin
        loop do
          select
          when msg = channel.receive?
            if msg
              unless msg.body.empty? && msg.headers.try(&.["Status"]?) == "503"
                yield msg
              end
            end
            if start.elapsed >= original_timeout
              return
            end
            timeout = original_timeout - start.elapsed
          when timeout(timeout)
            channel.close
            return
          end
        end
      ensure
        remove_handler key
      end
    end

    # Make an asynchronous request to subscribers of the given `subject`, not
    # waiting for a response. The first message to come back will be passed to
    # the block.
    def request(subject : String, message : Data = "", timeout = 2.seconds, &block : Message ->) : Nil
      inbox = @nuid.next
      key = "#{@inbox_prefix}.#{inbox}"
      add_handler key do |msg|
        block.call msg
        remove_handler key
      end
      publish subject, message, reply_to: key

      # TODO: Don't spin up a new fiber for each async request. We should figure
      # out a way to only have one fiber handling all async requests.
      spawn remove_handler(key, after: timeout)
    end

    protected def add_handler(key : String, &block : Message ->) : Nil
      @handler_mutex.synchronize do
        @inbox_handlers[key] = block
      end
    end

    protected def get_handler(key : String) : (Message ->)?
      @handler_mutex.synchronize do
        @inbox_handlers[key]?
      end
    end

    protected def remove_handler(key : String, after timeout : Time::Span? = nil) : Nil
      sleep timeout if timeout

      @handler_mutex.synchronize do
        @inbox_handlers.delete key
      end
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
    @[Deprecated("Use the `publish` method where the payload is called `data`. This matches NATS clients in other languages.")]
    def publish(subject : String, *, message : Data = Bytes.empty, reply_to : String? = nil, headers : Message::Headers? = nil) : Nil
      publish subject, message, reply_to, headers
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
    def publish(subject : String, data : Data = Bytes.empty, reply_to : String? = nil, headers : Message::Headers? = nil) : Nil
      validate_publish_subject! subject

      if subject.includes? ' '
        raise ArgumentError.new("Cannot publish to a subject that contains a space")
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
      else
        header_length = 0
      end

      if data.bytesize + header_length > @server_info.max_payload
        raise Error.new("Payload exceeds maximum size allowed by the NATS server")
      end

      LOG.debug { "Publishing #{data.bytesize} bytes to #{subject.inspect}, reply_to: #{reply_to.inspect}, headers: #{headers.inspect}" }
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
          @io << ' ' << header_length
          @io << ' ' << header_length + data.bytesize << "\r\n"
          @io << nats_header_preamble
          headers.each do |key, values|
            values.each do |value|
              @io << key << ": " << value << "\r\n"
            end
          end
          @io << "\r\n"
        else
          @io << ' ' << data.bytesize << "\r\n"
        end

        @io.write data.to_slice
        @io << "\r\n"
      end
    end

    def drain
      flush

      # Snapshot the list of subscriptions because we're about to remove them
      # from the hash
      subscriptions = @subscriptions.values

      # Make sure we don't get any new messages
      subscriptions.each { |subscription| unsubscribe subscription.sid }
      flush

      # Run through the snapshot of subscriptions we took above and wait until
      # we process all of the messages in memory.
      subscriptions.each(&.drain)
    end

    # Flush the client's output buffer over the wire and wait for the server to
    # acknowledge it. Raises `NATS::Error` if the server does not acknowledge
    # within `timeout` or if the connection is lost while waiting. If the client
    # is disconnected when this is called, the flush completes once the client
    # reconnects and the buffered data has been acknowledged, if that happens
    # within the `timeout`.
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
    rescue Channel::ClosedError
      raise Error.new("Connection to the NATS server was lost before the flush completed")
    end

    # Flush the client's output buffer over the wire without waiting for the
    # server to acknowledge it. This is a no-op while disconnected; the buffered
    # data is sent as soon as the client reconnects.
    def flush!
      @out.synchronize do
        @socket.flush if @state.connected?
        @data_waiting = false
      end
    rescue ex : IO::Error
      LOG.warn(exception: ex) { "Connection to the NATS server lost while flushing" }
      handle_disconnect!
    end

    def ping(channel = Channel(Nil).new(1))
      LOG.trace { "Sending PING" }
      @out.synchronize do
        write { @io << "PING\r\n" }
        # Register the waiter only after the write succeeded so a write that had
        # to be retried after a disconnect doesn't register it twice. Holding
        # `@out` keeps the order of waiters consistent with the order of PINGs.
        @pings_mutex.synchronize { @pings.push channel }
        @ping_count.add 1
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
        break if @state.closed?
        next unless @state.connected?

        if @ping_count.get > @max_pings_out
          LOG.warn { "Too many unresolved pings. Reconnecting..." }
          handle_disconnect!
        else
          ping
        end
      rescue ClientClosed
        break
      rescue ex
        emit_error ex
      end
    end

    private OUTBOUND_INTERVAL = ENV
      .fetch("NATS_FLUSH_INTERVAL_MS", "10")
      .to_i { 10 } # if we can't parse an int, just use 10
      .milliseconds

    private def begin_outbound
      loop do
        sleep OUTBOUND_INTERVAL
        break if @state.closed?

        if data_waiting? && @state.connected?
          LOG.trace { "Flushing output buffer..." }
          flush!
          LOG.trace { "Output flushed." }
        end
      rescue ex
        emit_error ex
      end
    end

    private def begin_inbound
      loop do
        break if @state.closed?
        unless @state.connected?
          # Waiting on the reconnect fiber to establish a new connection
          sleep 1.millisecond
          next
        end

        begin
          handle_inbound_command
        rescue ex : IO::Error
          break if @state.closed?
          if @state.connected?
            if @server_info.lame_duck_mode?
              # This is the expected end of lame-duck mode, not a failure
              LOG.info { "Lame-duck server #{@server_info.server_name} closed the connection, reconnecting" }
            else
              LOG.warn(exception: ex) { "Connection to the NATS server lost" }
            end
          end
          handle_disconnect!
        rescue ex
          break if @state.closed?
          # We can't trust our position in the protocol stream after an
          # unexpected error, so treat it as a lost connection and start over.
          LOG.error(exception: ex) { "Unexpected error handling data from the NATS server, reconnecting" }
          emit_error ex
          handle_disconnect!
        end
      end
    ensure
      LOG.debug { "Exited inbound message loop" }
    end

    # Read and process a single command from the server
    private def handle_inbound_command : Nil
      # The parser is a struct wrapping the socket, so building one per event
      # costs nothing and — unlike hoisting it out of the loop — picks up the
      # new socket after a reconnect.
      event = EventParser.new(@socket).call
      case event
      in Protocol::MessageEvent
        deliver event.message, to: event.sid
      in Protocol::OKEvent
        # Cool, thanks
      in Protocol::PingEvent
        pong
      in Protocol::PongEvent
        # `Atomic#sub` returns the value before subtracting
        if @ping_count.sub(1) > 0
          if channel = @pings_mutex.synchronize { @pings.shift? }
            channel.send nil rescue Channel::ClosedError
          end
        else
          # Nothing was waiting on this PONG (for example, its PING was
          # abandoned in a disconnect), so undo the decrement.
          @ping_count.add 1
          LOG.debug { "Received PONG without an outstanding PING" }
        end
      in Protocol::InfoEvent
        was_lame_duck = @server_info.lame_duck_mode?
        @server_info = event.server_info
        if (urls = @server_info.connect_urls).any?
          @discovered_servers = urls.map { |host_with_port| discovered_server_uri(host_with_port) }
        end
        if @server_info.lame_duck_mode? && !was_lame_duck
          LOG.info { "NATS server #{@server_info.server_name} entered lame duck mode" }
          # Don't let a slow handler hold up reading from the socket
          spawn name: "NATS::Client#on_lame_duck_mode (#{object_id})" do
            begin
              @on_lame_duck_mode.call
            rescue ex
              emit_error ex
            end
          end
        end
      in Protocol::ErrorEvent, Protocol::UnknownEvent
        emit_error event.error
      in Protocol::Event
        # Unreachable — `Event` is abstract. Crystal collapses the union of
        # its subclasses back into the virtual type, though, so exhaustiveness
        # demands a branch for the base itself. It also means a newly added
        # event type lands here rather than failing to compile, so route it to
        # the error handler instead of letting it disappear.
        emit_error UnknownCommand.new("Unhandled protocol event: #{event.type}")
      end
      Fiber.yield
    end

    # The wire protocol carries SIDs as opaque tokens, but we only ever hand the
    # server the integer SIDs we generate ourselves, so anything else is a
    # subscription we don't know about.
    private def deliver(msg : Message, to sid : String) : Nil
      unless (id = sid.to_i64?) && (subscription = @subscriptions[id]?)
        LOG.debug { "No subscription #{sid}" }
        return
      end

      begin
        subscription.send msg do |ex|
          LOG.trace { "Error occurred in handling subscription #{sid}: #{ex}" }
          emit_error ex
        end
      rescue Channel::ClosedError
        # The subscription was closed while we were delivering to it.
        # That's fine, the message is simply dropped.
        LOG.debug { "Subscription #{sid} closed while delivering a message" }
      end
      if subscription.messages_remaining
        LOG.trace { "Messages remaining in subscription #{sid} to #{subscription.subject}: #{subscription.messages_remaining}" }
      end
      if (messages_remaining = subscription.messages_remaining) && messages_remaining <= 0
        @subscriptions.delete id
        subscription.close
      end
    end

    # Close this NATS connection. This should be done explicitly before exiting
    # the program so that the NATS server can remove any subscriptions that were
    # associated with this client.
    #
    # If the client is currently connected, it drains its subscriptions before
    # closing. If it is disconnected, any data buffered while waiting to
    # reconnect is discarded and the client stops trying to reconnect.
    def close : Nil
      return if @state.closed?

      if @state.connected?
        LOG.trace { "Flushing/draining before closing..." }
        begin
          drain
        rescue ex : Error | IO::Error
          LOG.debug(exception: ex) { "Could not drain cleanly before closing" }
        end
      end

      @out.synchronize do
        @state = :closed
        @socket.close rescue nil
        @disconnect_buffer = IO::Memory.new
        @io = @disconnect_buffer
      end
      # Wake up anything waiting on a flush so it doesn't hang until timeout
      fail_pending_pings
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

    @on_lame_duck_mode = -> { }

    # Execute the given block when the NATS server this client is connected to
    # enters lame-duck mode, meaning it is shutting down and will close this
    # connection at some point within its lame-duck duration. The client will
    # reconnect to another server automatically when that happens, but you can
    # use this to stop taking on new work or to drain in-flight work first.
    #
    # ```
    # nats = NATS::Client.new
    # nats.on_lame_duck_mode { Log.info { "NATS server is shutting down" } }
    # ```
    def on_lame_duck_mode(&@on_lame_duck_mode)
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

    # Whether outgoing data is currently being held in memory because we are
    # not connected to the server. Must be called while holding `@out` for the
    # answer to be reliable.
    private def buffering? : Bool
      @io.same? @disconnect_buffer
    end

    # Write protocol data to the server. While connected, the block writes to
    # the socket. While disconnected, it writes to an in-memory buffer that is
    # replayed once we reconnect, so this never blocks on the connection being
    # re-established.
    private def write(&) : Nil
      # Bound the number of retries so that a socket that keeps failing can't
      # spin us forever. In practice a single retry lands the write in the
      # disconnect buffer, which can't raise `IO::Error`.
      3.times do
        if @state.closed?
          raise ClientClosed.new("Client has been explicitly closed.")
        end
        # Somebody closed the socket out from under us. Make sure we've noticed.
        handle_disconnect! if @socket.closed?

        @out.synchronize do
          if buffering? && @disconnect_buffer.bytesize >= @reconnect_buffer_size
            raise ReconnectBufferExceeded.new("Buffered #{@disconnect_buffer.bytesize} bytes while disconnected from NATS, exceeding the limit of #{@reconnect_buffer_size} bytes")
          end
          yield
          @data_waiting = true
        end
        return
      rescue ex : IO::Error
        # The socket died mid-write. Switch to the disconnect buffer (unless
        # another fiber already has) and retry, which will land the write there.
        LOG.warn(exception: ex) { "Connection to the NATS server lost while writing" }
        handle_disconnect!
      end

      raise Error.new("Could not write to the NATS server")
    end

    # Transition from connected to disconnected, if we haven't already, and kick
    # off the reconnect process in the background. This is safe to call from any
    # fiber, does not block, and is a no-op if the client is already
    # disconnected, reconnecting, or closed.
    private def handle_disconnect! : Nil
      transitioned = @out.synchronize do
        if @state.connected?
          @state = :disconnected
          @socket.close rescue nil
          # Redirect all writes to the buffer until we reconnect to the server
          @io = @disconnect_buffer
          @data_waiting = false
          LOG.trace { "Output set to in-memory buffer pending reconnection" }

          # Nobody is going to answer the PINGs we sent on the old connection
          fail_pending_pings

          if (connected_at = @connected_at) && connected_at.elapsed >= STABLE_CONNECTION_DURATION
            @reconnect_backoff = MIN_RECONNECT_BACKOFF
          end
          true
        else
          false
        end
      end

      if transitioned
        spawn name: "NATS::Client#reconnect (#{object_id})" do
          begin
            @on_disconnect.call
          rescue ex
            emit_error ex
          end
          reconnect_loop
        end
      end
    end

    # Close every channel waiting on a PONG so that the fibers blocked in
    # `flush` fail fast instead of waiting for their timeouts, and reset the
    # outstanding ping count.
    private def fail_pending_pings : Nil
      @pings_mutex.synchronize do
        while channel = @pings.shift?
          channel.close
        end
        @ping_count.set 0
      end
    end

    # Keep trying to reconnect with exponential backoff until we succeed or the
    # client is closed. Runs in its own fiber.
    private def reconnect_loop : Nil
      until @state.closed?
        backoff = @reconnect_backoff
        @reconnect_backoff = {backoff * 2, MAX_RECONNECT_BACKOFF}.min
        sleep backoff
        break if @state.closed?

        @out.synchronize do
          @state = :reconnecting if @state.disconnected?
        end
        break unless @state.reconnecting?

        LOG.warn &.emit "Reconnecting", servers: candidate_servers.map(&.to_s)
        begin
          reconnect!
          break
        rescue ex
          LOG.warn(exception: ex) { "Reconnect attempt failed, retrying in #{@reconnect_backoff}" }
          emit_error ex
          @out.synchronize do
            @state = :disconnected if @state.reconnecting?
          end
        end
      end
    end

    # Make a single attempt to establish a new connection and, if it succeeds,
    # swap it in: re-establish all subscriptions on it, replay everything that
    # was buffered while we were disconnected, and go back to the connected
    # state.
    private def reconnect! : Nil
      socket, server_info = connect

      @out.synchronize do
        if @state.closed?
          socket.close rescue nil
          return
        end

        begin
          @subscriptions.each_value do |subscription|
            LOG.trace { "Resubscribing to #{subscription.subject}#{" (queue_group: #{subscription.queue_group})" if subscription.queue_group} with sid #{subscription.sid}..." }
            write_subscription subscription, to: socket
          end
          LOG.trace { "Replaying #{@disconnect_buffer.bytesize} bytes buffered while disconnected" }
          socket.write @disconnect_buffer.to_slice
          socket.flush
        rescue ex
          # Keep the buffered data for the next attempt
          socket.close rescue nil
          raise ex
        end

        @socket = socket
        @io = socket
        @server_info = server_info
        @disconnect_buffer = IO::Memory.new
        @data_waiting = false
        @connected_at = Time.instant
        @state = :connected
      end
      LOG.info { "Reconnected to NATS server #{server_info.server_name}" }

      begin
        @on_reconnect.call
      rescue ex
        emit_error ex
      end
    end

    # Every server we might connect to: the ones the cluster has told us about
    # plus the ones we were configured with.
    private def candidate_servers : Array(URI)
      (@discovered_servers + @servers).uniq
    end

    # `connect_urls` entries are just `host:port`, so we borrow the scheme,
    # credentials, and connection name from the configured servers. Otherwise
    # reconnecting to a discovered server would drop auth, TLS, and the name.
    private def discovered_server_uri(host_with_port : String) : URI
      template = @servers.first
      uri = URI.parse("#{template.scheme}://#{host_with_port}")
      uri.user = template.user
      uri.password = template.password
      uri.path = template.path
      uri
    end

    # Invoke the `on_error` callback without letting an exception inside it take
    # down one of the client's internal fibers.
    private def emit_error(error : Exception) : Nil
      @on_error.call error
    rescue handler_error
      LOG.error(exception: handler_error) { "The on_error handler raised while handling #{error.class}: #{error.message}" }
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
