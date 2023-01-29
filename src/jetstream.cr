require "json"
require "./nats"
require "./error"

module NATS
  # NATS JetStream provides at-least-once delivery guarantees with the
  # possibility of exactly-once for some use cases, allowing NATS to be used for
  # scenarios where 100% delivery of messages and events is required.
  module JetStream
    class Error < ::NATS::Error
    end

    # https://github.com/nats-io/nats-server/blob/main/server/errors.json
    enum Errors : Int32 # Should be wide enough?
      None                              =     0
      ConsumerNotFound                  = 10014
      NoMessageFound                    = 10037
      StreamNotFound                    = 10059
      StreamWrongLastSequence           = 10071
      MaximumMessagesPerSubjectExceeded = 10077

      def self.new(json : JSON::PullParser)
        new json.read_int.to_i
      end
    end

    # This class provides a client for NATS JetStream for at-least-once delivery.
    # You can either instantiate it with a NATS client or with the
    # `NATS::Client#jetstream` method as a shortcut.
    class Client
      def initialize(@nats : ::NATS::Client)
      end

      # Returns an `API::Stream` instance to interact with the NATS JetStream
      # API for streams.
      def stream
        Streams.new(@nats)
      end

      # Returns an `API::Consumer` instance to interact with the NATS JetStream
      # API for consumers.
      def consumer
        Consumers.new(@nats)
      end

      def publish(
        subject : String,
        body : Data,
        timeout : Time::Span = 2.seconds,
        headers : Headers = Headers.new,
        message_id : String? = nil,
        expected_last_message_id : String? = nil,
        expected_last_sequence : Int64? = nil,
        expected_stream : String? = nil,
        expected_last_subject_sequence : Int64? = nil
      )
        headers["Nats-Msg-Id"] = message_id if message_id
        headers["Nats-Expected-Last-Msg-Id"] = expected_last_message_id if expected_last_message_id
        headers["Nats-Expected-Stream"] = expected_stream if expected_stream
        headers["Nats-Expected-Last-Sequence"] = expected_last_sequence if expected_last_sequence
        headers["Nats-Expected-Last-Subject-Sequence"] = expected_last_subject_sequence.to_s if expected_last_subject_sequence

        if response = @nats.request(subject, body, timeout: timeout, headers: headers)
          (API::V1::PubAck | API::V1::ErrorResponse).from_json(String.new(response.body))
        end
      end

      # Subscribe to messages delivered to the given consumer. Note that this
      # consumer _must_ be a push-based consumer. Pull-based consumers do not
      # allow subscriptions because you must explicitly request the next
      # message.
      #
      # ```
      # js = nats.jetstream
      # consumer = js.consumer.info("orders", "fulfillment")
      # js.subscribe consumer do |msg|
      #   # ...
      #
      #   js.ack msg
      # end
      # ```
      def subscribe(consumer : JetStream::API::V1::Consumer, &block : Message ->)
        if subject = consumer.config.deliver_subject
          subscribe subject, queue_group: consumer.config.deliver_group, &block
        else
          raise ArgumentError.new("Consumer is not a push consumer (no `deliver_subject`)")
        end
      end

      # Subscribe to the given subject with an optional queue group. This is
      # effectively identical to `NATS::Client#subscribe`, but the message
      # yielded to the block is a `NATS::JetStream::Message` instead of
      # a `NATS::Message`.
      #
      # ```
      # js = nats.jetstream
      # js.subscribe "orders.*", queue_group: "fulfillment" do |msg|
      #   # ...
      #
      #   js.ack msg
      # end
      # ```
      #
      # _NOTE:_ If provided, the `queue_group` _must_ be the same as a `Consumer`'s `deliver_group` for NATS server 2.4.0 and above.
      def subscribe(subject : String, queue_group : String? = nil, &block : Message ->)
        @nats.subscribe subject, queue_group: queue_group do |msg|
          unless (headers = msg.headers) && headers["Status"]? == "100 Idle Heartbeat"
            block.call Message.new(msg)
          end
        end
      end

      @[Experimental("NATS JetStream pull subscriptions may be unstable")]
      def pull_each(consumer : API::V1::Consumer, *, batch_size : Int) : Nil
        pull_subscribe consumer, batch_size: batch_size do |msgs, pull|
          msgs.each { |msg| yield msg, pull }
        end
      end

      @[Experimental("NATS JetStream pull subscriptions may be unstable")]
      def pull_subscribe(consumer : API::V1::Consumer, *, batch_size : Int) : Nil
        pull = pull_subscribe(consumer, backlog: batch_size)
        until pull.closed?
          yield pull.fetch(batch_size), pull
        end
      end

      @[Experimental("NATS JetStream pull subscriptions may be unstable")]
      def pull_subscribe(consumer : API::V1::Consumer, backlog : Int = 64)
        if consumer.config.deliver_subject
          raise ArgumentError.new("Cannot set up a pull-based subscription to a push-based consumer: #{consumer.config.durable_name.inspect} on stream #{consumer.stream_name.inspect}")
        end

        subject = "pull-subscriber.#{consumer.stream_name}.#{consumer.name}.#{NUID.next}"
        channel = Channel(Message).new(backlog)

        start = Time.monotonic
        first_heartbeat = nil
        subscription = @nats.subscribe(subject, queue_group: consumer.config.deliver_group || consumer.name) do |msg, sub|
          if msg.reply_to.presence
            channel.send Message.new(msg)
          else
            # If there isn't a reply-to, we're probably getting an update about
            # some logistics pertaining to this pull subscription.
            if (headers = msg.headers) && headers["Status"]? == "409 Consumer Deleted"
              @nats.unsubscribe sub
            end
          end
        end

        PullSubscription.new(subscription, consumer, channel, @nats)
      end

      # Acknowledge success processing the specified message, usually called at
      # the end of your subscription block.
      #
      # NOTE: This method is asynchronous. If you need a guarantee that NATS has
      # received your acknowledgement, use `ack_sync` instead.
      #
      # ```
      # jetstream.subscribe consumer do |msg|
      #   # ...
      #
      #   jetstream.ack msg
      # end
      # ```
      def ack(msg : Message)
        @nats.publish msg.reply_to, "+ACK"
      end

      # Acknowledge the given message, waiting on the NATS server to acknowledge
      # your acknowledgement so that you can be sure it was delivered to the
      # server and not simply caught up in the output buffer.
      #
      # ```
      # jetstream.subscribe consumer do |msg|
      #   # ...
      #
      #   jetstream.ack_sync msg
      #   # By the time you get here, the NATS server knows you've acknowledged.
      # end
      # ```
      def ack_sync(msg : Message, timeout : Time::Span = 2.seconds)
        @nats.request msg.reply_to, "+ACK", timeout: timeout
      end

      def ack_sync(messages : Enumerable(Message), timeout : Time::Span = 2.seconds)
        channel = Channel(Bool).new(messages.size)
        messages.each do |msg|
          @nats.request msg.reply_to, "+ACK", timeout: timeout do |response|
            channel.send true
          end
        end

        count = 0
        messages.each do
          select
          when channel.receive
            count += 1
          when timeout(timeout)
          end
        end
        count
      end

      # Notify the NATS server that you need more time to process this message,
      # usually used when a consumer requires that you acknowledge a message
      # within a certain amount of time but a given message is taking longer
      # than expected.
      #
      # ```
      # jetstream.subscribe consumer do |msg|
      #   start = Time.monotonic
      #   users.each do |user|
      #     # If we're running out of time, reset the timer.
      #     if Time.monotonic - start > 25.seconds
      #       jetstream.in_progress msg
      #     end
      #
      #     process(msg)
      #   end
      # end
      # ```
      def in_progress(msg : Message)
        @nats.publish msg.reply_to, "+WPI"
      end

      # Negatively acknowledge the processing of a message, typically called
      # when an exception is raised while processing.
      #
      # ```
      # jetstream.subscribe consumer do |msg|
      #   # doing some work
      #
      #   jetstream.ack msg # Successfully processed
      # rescue ex
      #   jetstream.nack msg # Processing was unsuccessful, try again.
      # end
      # ```
      def nack(msg : Message)
        @nats.publish msg.reply_to, "-NAK"
      end

      # Deliver a negative acknowledgement for the given message and tell the
      # NATS server to delay sending it based on the `NAKBackoff` pattern
      # specified.
      #
      # ```
      # jetstream.subscribe consumer do |msg|
      #   # do some work
      #   jetstream.ack msg
      # rescue
      #   # Use exponential backoff of up to an hour for retries
      #   jetstream.nack msg, backoff: :exponential, max: 1.hour
      # end
      # ```
      def nack(msg : Message, *, backoff : NAKBackoff, max : Time::Span = 1.day)
        case backoff
        in .exponential?
          delay = {(2.0**(msg.delivered_count - 5)).seconds, max}.min
        in .linear?
          delay = {msg.delivered_count.seconds, max}.min
        end

        nack msg, delay: delay
      end

      # Deliver a negative acknowledgement for the given message and tell the
      # NATS server to delay sending it for the given time span.
      #
      # ```
      # jetstream.subscribe consumer do |msg|
      #   # do some work
      #   jetstream.ack msg
      # rescue
      #   # Use exponential backoff of up to an hour for retries
      #   jetstream.nack msg, delay: 30.seconds
      # end
      # ```
      def nack(msg : Message, *, delay : Time::Span)
        @nats.publish msg.reply_to, %(-NAK {"delay":#{delay.total_nanoseconds.to_i64}})
      end

      # Common `nack` backoff strategies give a simple shorthand for setting the
      # delays on subsequent delivery attempts.
      #
      # ```
      # nats = NATS::Client.new
      #
      # jetstream.subscribe consumer do |msg|
      #   # do some work
      #   jetstream.ack msg
      # rescue ex
      #   jetstream.nack msg, backoff: :exponential
      #   raise ex
      # end
      # ```
      enum NAKBackoff
        # Exponential backoff starts at a delay of 1/16th of a second and
        # doubles every time the message is nacked with this strategy. This has
        # the effect of running the first 3 attempts roughly immediately so that
        # an uncommon but not unexpected failure (API call in a downstream
        # service returns a 500) doesn't need to wait several seconds unless it
        # truly needs to.
        Exponential

        # Linear backoff delays for 1 second for each time the message is nacked
        # with this strategy.
        Linear
      end
    end

    # A `NATS::JetStream::Message` is very similar to a `NATS::Message` in that
    # it represents a piece of information published by a NATS client (not
    # necessarily _this_ NATS client, though). This `Message` type contains more
    # information, however, such as information about the stream and consumer
    # it came from, how many times it's been delivered, etc.
    struct Message
      # The name of the stream this message was consumed from
      getter stream : String

      # The name of the consumer we received this message from
      getter consumer : String

      # The number of times this particular message has been delivered by this
      # consumer, starting at 1
      getter delivered_count : Int64

      # The position of this message within its stream
      getter stream_seq : Int64

      # The position of this message within its consumer, including redeliveries
      getter consumer_seq : Int64

      # When this message was originally published
      getter timestamp : Time

      # How many messages follow this message for this consumer
      getter pending : Int64

      # The original body of the message, encoded as binary. If you need text,
      # wrap the body in a `String`.
      #
      # ```
      # jetstream.subscribe consumer do |msg|
      #   body_string = String.new(msg.body)
      #
      #   # ...
      # end
      # ```
      getter body : Bytes

      # The original subject this message was published to, which can be (and
      # most likely is) different from the subject it was delivered to
      getter subject : String

      # The subject used for acknowledging this message
      getter reply_to : String

      # Any headers that were published with this message, including ones
      # interpreted by the NATS server, such as `Nats-Msg-Id` for message
      # deduplication.
      getter headers : Headers { Headers.new }

      # Instantiate a `NATS::JetStream::Message` based on a `NATS::Message`.
      # Used by JetStream subscriptions to build `JetStream::Message`
      # instances, since JetStream is a layer on top of core NATS.
      def self.new(msg : ::NATS::Message)
        # reply_to format:
        # $JS.ACK.<stream>.<consumer>.<delivered count>.<stream sequence>.<consumer sequence>.<timestamp>.<pending messages>
        if reply_to = msg.reply_to
          # TODO: figure out if it's worth optimizing to avoid the array allocation
          _jetstream, _ack, stream, consumer, delivered_count, stream_seq, consumer_seq, timestamp, pending_messages = reply_to.split('.')
          new(
            stream: stream,
            consumer: consumer,
            delivered_count: delivered_count.to_i64,
            stream_seq: stream_seq.to_i64,
            consumer_seq: consumer_seq.to_i64,
            timestamp: Time::UNIX_EPOCH + timestamp.to_i64.nanoseconds,
            pending: pending_messages.to_i64,
            body: msg.body,
            subject: msg.subject,
            reply_to: reply_to,
            headers: msg.headers,
          )
        else
          raise InvalidJetStreamMessage.new("JetStream messages must have the reply_to field")
        end
      end

      def initialize(@stream, @consumer, @delivered_count, @stream_seq, @consumer_seq, @timestamp, @pending, @body, @subject, @reply_to, @headers)
      end

      class InvalidJetStreamMessage < Exception
      end
    end

    alias Streams = API::V1::Streams
    alias Consumers = API::V1::Consumers
    alias Placement = API::V1::StreamConfig::Placement
    alias StreamSource = API::V1::StreamSource
    alias ExternalStream = API::V1::ExternalStream

    module API
      abstract struct Message
        include JSON::Serializable
      end

      module V1
        struct ErrorResponse
          include JSON::Serializable

          getter error : Error

          struct Error
            include JSON::Serializable

            getter code : Int32
            getter err_code : Errors = :none
            getter description : String
          end
        end

        struct PubAck
          include JSON::Serializable

          getter stream : String
          @[JSON::Field(key: "seq")]
          getter sequence : Int64
          getter? duplicate : Bool = false
          getter domain : String?
        end

        # A stream in NATS JetStream represents the history of messages
        # pertaining to a given domain. When you publish a message to a subject
        # that a stream is monitoring, the stream then adds that message to its
        # history in the order it was published.
        struct Streams
          def initialize(@nats : ::NATS::Client)
          end

          # Create a stream of the given storage type and with the given
          # properties, which are passed unmodified to
          # `NATS::JetStream::API::V1::StreamConfig.new`.
          def create(
            storage : StreamConfig::Storage,
            retention : StreamConfig::RetentionPolicy? = nil,
            discard : StreamConfig::DiscardPolicy? = nil,
            **kwargs
          )
            create_stream = JetStream::API::V1::StreamConfig.new(
              **kwargs,
              storage: storage,
              retention: retention,
              discard: discard,
            )

            if create_stream.name.includes? '.'
              raise JetStream::Error.new("Cannot create stream with '.' in the name")
            end

            if response = @nats.request "$JS.API.STREAM.CREATE.#{create_stream.name}", create_stream.to_json
              case parsed = (JetStream::API::V1::Stream | ErrorResponse).from_json String.new(response.body)
              when ErrorResponse
                raise JetStream::Error.new("#{parsed.error.description} (#{parsed.error.code})")
              else
                parsed
              end
            else
              raise JetStream::Error.new("Did not receive a response from NATS JetStream")
            end
          rescue ex
            raise ex
          end

          # List all available streams
          def list(subject : String? = nil)
            if response = @nats.request "$JS.API.STREAM.LIST", {subject: subject}.to_json
              NATS::JetStream::API::V1::StreamListResponse.from_json(String.new(response.body))
            else
              raise "whoops"
            end
          end

          # Get the current state of the stream with the given `name`
          def info(name : String) : Stream?
            if response = @nats.request "$JS.API.STREAM.INFO.#{name}"
              case parsed = (Stream | ErrorResponse).from_json(String.new(response.body))
              in Stream
                parsed
              in ErrorResponse
                if parsed.error.err_code.stream_not_found?
                  nil
                else
                  raise Error.new(parsed.error.description)
                end
              end
            else
              raise Error.new("Response timed out while fetching stream #{name.inspect}")
            end
          end

          # Delete the given stream
          def delete(stream : JetStream::API::V1::Stream)
            delete stream.config.name
          end

          # Delete the stream with the given name
          def delete(stream : String)
            @nats.request "$JS.API.STREAM.DELETE.#{stream}"
          end

          def get_msg(stream : String, *, last_by_subject : String)
            get_msg stream, {last_by_subj: last_by_subject}
          end

          def get_msg(stream : String, *, sequence : Int, next_by_subject : String? = nil)
            get_msg stream, {seq: sequence, next_by_subj: next_by_subject}
          end

          protected def get_msg(stream : String, params)
            if response = @nats.request "$JS.API.STREAM.MSG.GET.#{stream}", params.to_json
              case parsed = (StreamGetMsgResponse | ErrorResponse).from_json String.new(response.body)
              in StreamGetMsgResponse
                parsed
              in ErrorResponse
                if parsed.error.err_code.no_message_found?
                  nil # No message
                else
                  raise Error.new(parsed.error.description)
                end
              end
            else
              raise Error.new("Did not receive a response when getting message from stream #{stream.inspect} with options #{params}")
            end
          end

          def purge(stream : String, subject : String) : Int64
            if response = @nats.request("$JS.API.STREAM.PURGE.#{stream}", {filter: subject}.to_json)
              case parsed = (PurgeStreamResponse | ErrorResponse).from_json String.new(response.body)
              in PurgeStreamResponse
                parsed.purged
              in ErrorResponse
                raise Error.new(parsed.error.description)
              end
            else
              raise Error.new("Did not receive a response when purging stream #{stream.inspect} of subject #{subject.inspect}")
            end
          end

          struct PurgeStreamResponse
            include JSON::Serializable

            getter purged : Int64
          end
        end

        # A NATS JetStream consumer is a message index sourced from a stream.
        # It can apply additional filters and records which messages are pending,
        # acknowledged, etc, at the consumer layer.
        struct Consumers
          def initialize(@nats : ::NATS::Client)
          end

          # Create a consumer for the given stream with the given properties,
          # which are passed unmodified to `NATS::JetStream::API::V1::Consumer.new`.
          def create(
            stream_name : String,
            deliver_policy : ConsumerConfig::DeliverPolicy = :all,
            ack_policy : ConsumerConfig::AckPolicy = :explicit,
            **properties
          ) : Consumer
            consumer_config = ConsumerConfig.new(
              **properties,
              deliver_policy: deliver_policy,
              ack_policy: ack_policy,
            )
            create_consumer = {stream_name: stream_name, config: consumer_config}
            if durable_name = consumer_config.durable_name
              create_consumer_subject = "$JS.API.CONSUMER.DURABLE.CREATE.#{stream_name}.#{durable_name}"
            else
              create_consumer_subject = "$JS.API.CONSUMER.CREATE.#{stream_name}"
            end

            unless response = @nats.request create_consumer_subject, create_consumer.to_json
              raise JetStream::Error.new("Did not receive a response from NATS JetStream")
            end

            case parsed = (Consumer | ErrorResponse).from_json String.new(response.body)
            in Consumer
              parsed
            in ErrorResponse
              raise JetStream::Error.new("#{parsed.error.description} (#{parsed.error.code})")
            end
          end

          # Returns a paginated list of consumers for the specified stream.
          def list(stream : JetStream::API::V1::Stream)
            list stream.config.name
          end

          # Returns a paginated list of consumers for the stream with the
          # specified name.
          def list(stream_name : String)
            if consumers_response = @nats.request "$JS.API.CONSUMER.LIST.#{stream_name}"
              NATS::JetStream::API::V1::ConsumerListResponse.from_json(String.new(consumers_response.body))
            else
              raise "whoops"
            end
          end

          # Return the consumer with the specified `name` associated with the
          # given stream, yielding to the block if the consumer does not exist
          def info!(stream_name : String, name : String) : Consumer
            info(stream_name, name) do
              raise ArgumentError.new("Consumer #{name.inspect} not found for stream #{stream_name.inspect}")
            end
          end

          def info(stream_name : String, name : String) : Consumer?
            info(stream_name, name) { nil }
          end

          # Return the consumer with the specified `name` associated with the
          # given stream, yielding to the block if the consumer does not exist
          def info(stream_name : String, name : String)
            if consumer_response = @nats.request "$JS.API.CONSUMER.INFO.#{stream_name}.#{name}"
              case parsed = (Consumer | ErrorResponse).from_json(String.new(consumer_response.body))
              in Consumer
                parsed
              in ErrorResponse
                if parsed.error.err_code.consumer_not_found?
                  yield
                else
                  raise Error.new(parsed.error.description)
                end
              end
            else
              raise "no info for #{name.inspect} (stream #{stream_name.inspect})"
            end
          end

          # Delete the given consumer for the given stream
          def delete(consumer : JetStream::API::V1::Consumer)
            delete consumer.stream_name, consumer.name
          end

          # Delete the given consumer for the given stream
          def delete(stream : JetStream::API::V1::Stream, consumer : JetStream::API::V1::Consumer)
            delete stream.config.name, consumer.name
          end

          # Delete the consumer with the given name associated with the stream
          # with the given name.
          def delete(stream : String, consumer : String)
            @nats.request "$JS.API.CONSUMER.DELETE.#{stream}.#{consumer}"
          end
        end

        struct Stream < Message
          getter config : StreamConfig
          getter created : Time
          getter state : StreamState
          getter cluster : ClusterInfo?
          getter mirror : StreamSourceInfo?
          getter sources : Array(StreamSourceInfo) { [] of StreamSourceInfo }
        end

        struct StreamSource < Message
          getter name : String
          getter opt_start_seq : UInt64?
          getter opt_start_time : Time?
          getter filter_subject : String?
          getter external : ExternalStream?

          def initialize(
            @name,
            @opt_start_seq = nil,
            @opt_start_time = nil,
            @filter_subject = nil,
            @external = nil
          )
          end
        end

        struct StreamSourceInfo < Message
          getter name : String
          getter external : ExternalStream?
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter lag : Time::Span
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter active : Time::Span
          getter error : APIError?

          def initialize(@name, @external = nil, @lag = 0.seconds, @active = 0.seconds, @error = nil)
          end
        end

        struct ExternalStream < Message
          @[JSON::Field(key: "api")]
          getter api_prefix : String
          @[JSON::Field(key: "deliver")]
          getter deliver_prefix : String

          def initialize(*, @api_prefix = "", @deliver_prefix = "")
          end
        end

        struct APIError < Message
          getter code : Int64
          getter err_code : UInt16?
          getter description : String?
        end

        struct StreamListResponse < Message
          include Enumerable(Stream)

          getter total : Int64
          getter offset : Int64
          getter limit : Int64
          getter streams : Array(Stream)

          def each
            streams.each { |s| yield s }
          end
        end

        struct StreamGetMsgResponse < Message
          getter message : Message

          struct Message
            include JSON::Serializable
            getter subject : String
            getter seq : Int64
            @[JSON::Field(converter: ::NATS::JetStream::API::V1::StreamGetMsgResponse::Message::Base64Data)]
            getter data : Bytes = Bytes.empty
            @[JSON::Field(key: "hdrs", converter: ::NATS::JetStream::API::V1::StreamGetMsgResponse::Message::HeadersConverter)]
            getter headers : Headers { Headers.new }
            getter time : Time

            module Base64Data
              def self.from_json(json : JSON::PullParser)
                ::Base64.decode json.read_string
              end

              def self.to_json(json : JSON::Builder, value : Bytes)
                json.string ::Base64.encode(value)
              end
            end

            module HeadersConverter
              def self.from_json(json : JSON::PullParser)
                if string = json.read_string_or_null
                  # Decoded string will be in the format:
                  #   "NATS/1.0\r\nHeader1: Value1\r\nHeader2: Value2\r\n\r\n"
                  # So we want to omit the first line (preamble) and the last
                  # line (it's blank).
                  raw = Base64.decode_string(string)
                  header_count = raw.count('\n') - 2
                  headers = Headers.new(initial_capacity: header_count)

                  raw.each_line do |line|
                    if separator_index = line.index(':')
                      key = line[0...separator_index]
                      value = line[separator_index + 2..]
                      headers[key] = value
                    end
                  end

                  headers
                end
              end
            end
          end
        end

        struct StreamState < Message
          getter messages : Int64
          getter bytes : Int64
          getter first_seq : Int64
          getter first_ts : Time
          getter last_seq : Int64
          getter last_ts : Time
          getter consumer_count : Int32
        end

        struct StreamConfig < Message
          # The `Storage` parameter tells the NATS server how to store the
          # messages in the stream.
          enum Storage
            # Store messages in memory. This is the fastest, but is not durable.
            # If the NATS server is restarted for any reason, the stream will
            # be dropped. Using this option may also increase the amount of
            # memory required by your NATS server. Use with caution.
            Memory

            # Store messages on disk. Always use this if your system depends on
            # 100% delivery.
            File
          end

          # The `RetentionPolicy` tells the NATS server when messages can be
          # discarded. Your options are to wait until the a quantity/volume/time
          # limit has been reached, _all_ consumers have acknowledged, or _any_
          # consumers have acknowledged.
          #
          # ```
          # jetstream.stream.create(
          #   # ...
          #   retention: :workqueue,
          # )
          # ```
          enum RetentionPolicy
            # Discard messages when the stream has reached the limit of either
            # the number of messages or the total stream size in bytes, or the
            # message's max age has passed.
            Limits

            # Discard a message when all subscribed consumers have acknowledged
            # it to guarantee delivery but not keep the message in memory. This
            # is the default behavior of AMQP.
            Interest

            # Discard a message when the first consumer has acknowledged it, as
            # in a work queue like Sidekiq.
            Workqueue
          end

          # The `DiscardPolicy` tells the NATS server which end of the stream to
          # truncate when the stream is full — should we start dropping old
          # messages or avoid adding new ones?
          enum DiscardPolicy
            # Drop old messages when the stream has reached a count or volume
            # limit
            Old

            # Don't add new messages until old ones are discarded
            New
          end

          struct Placement < Message
            getter cluster : String?
            getter tags : Array(String) { %w[] }

            def initialize(@cluster = nil, @tags = nil)
            end
          end

          struct Republish < Message
            @[JSON::Field(key: "src")]
            getter source : String?
            @[JSON::Field(key: "dest")]
            getter destination : String
            getter? headers_only : Bool?

            def initialize(*, @source : String? = nil, @destination, @headers_only = nil)
            end
          end

          # Name of this stream
          getter name : String
          getter description : String?

          # Which subjects this stream will listen for.
          getter subjects : Array(String) { [] of String }
          getter storage : Storage
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter max_age : Time::Span?
          getter max_bytes : Int64?
          getter max_msg_size : Int32?
          getter max_msgs : Int64?
          getter max_msgs_per_subject : Int64?
          getter max_consumers : Int32?
          getter? no_ack : Bool?
          @[JSON::Field(key: "num_replicas")]
          getter replicas : Int32
          getter retention : RetentionPolicy?
          getter discard : DiscardPolicy?
          getter placement : Placement?
          getter mirror : StreamSource?
          getter sources : Array(StreamSource) { [] of StreamSource }
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter duplicate_window : Time::Span?
          @[JSON::Field(key: "allow_rollup_hdrs")]
          getter? allow_rollup_headers : Bool?
          getter? deny_purge : Bool?
          getter? deny_delete : Bool?
          getter? sealed : Bool?
          getter? allow_direct : Bool?
          getter? mirror_direct : Bool?
          getter? discard_new_per_subject : Bool?
          getter republish : Republish?

          def initialize(
            @name,
            @subjects = nil,
            @description = nil,
            @max_age = nil,
            @max_bytes = nil,
            @max_msg_size = nil,
            @max_msgs = nil,
            @max_msgs_per_subject = nil,
            @max_consumers = nil,
            @no_ack = false,
            @replicas = 1,
            @retention : RetentionPolicy? = nil,
            @allow_rollup_headers = nil,
            @deny_delete = nil,
            @allow_direct = nil,
            @mirror_direct = nil,
            @republish = nil,
            @placement = nil,
            @mirror = nil,
            @sources = nil,
            @discard_new_per_subject = nil,
            @discard : DiscardPolicy? = nil,
            @storage : Storage = :file
          )
          end
        end

        struct Consumer < Message
          # The name of the stream this consumer sources its messages from
          getter stream_name : String

          # The name of this consumer
          getter name : String

          # The timestamp when this consumer was created
          getter created : Time

          # The configuration used to create this consumer (including its defaults)
          getter config : ConsumerConfig

          # The number of times this consumer has delivered messages for this stream
          getter delivered : Sequence

          # The number of messages that have been acknowledged for this consumer/stream
          getter ack_floor : Sequence

          # The number of messages currently in-flight that are awaiting acknowledgement
          getter num_ack_pending : Int64

          # The number of messages that have been redelivered
          getter num_redelivered : Int64

          # The number of messages that are currently waiting
          getter num_waiting : Int64

          # The number of messages in the stream that this consumer has not delivered at all yet
          getter num_pending : Int64

          # Where this consumer's data lives in the cluster
          getter cluster : ClusterInfo?

          getter? push_bound : Bool = false

          # The sequence represense a cursor for how many messages have been
          # delivered or acknowledged for this consumer and stream.
          struct Sequence < Message
            getter consumer_seq : Int64
            getter stream_seq : Int64
          end
        end

        struct ConsumerConfig < Message
          # How messages should be acknowledged: none, all, or explicit
          getter ack_policy : AckPolicy
          # How long to allow messages to remain un-acknowledged before attempting redelivery
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter ack_wait : Time::Span?
          # The initial starting mode of the consumer: all, last, new, by-start_sequence or by_start_time
          getter deliver_policy : DeliverPolicy
          # The subject to deliver observed messages, when not set, a pull-based Consumer is created
          getter deliver_subject : String?
          # The name of the Consumer, specifying this will persist the consumer to the NATS server
          getter durable_name : String?

          getter deliver_group : String?
          getter description : String?
          getter max_waiting : Int64?
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter idle_heartbeat : Time::Span?
          getter? flow_control : Bool { false }
          getter? headers_only : Bool { false }

          # Apparently we're not supposed to use this in clients
          # See https://github.com/nats-io/nats-server/blob/3aa8e63b290ac4ba1c99193827b3f66ad5679904/server/consumer.go#L70-L71
          # getter? direct : Bool = false

          # When consuming from a Stream with many subjects, or wildcards, select only a specific incoming subjects, supports wildcards
          getter filter_subject : String?
          # Maximum number of times a message will be delivered via this consumer. Use this to avoid poison pills crashing all your services forever.
          getter max_deliver : Int64?
          # When first consuming messages from the Stream start at this particular message in the set
          getter opt_start_seq : Int64?
          # OptStartTime	When first consuming messages from the Stream start with messages on or after this time
          getter opt_start_time : Time?
          # How messages are sent: `instant` (default) or `original`
          getter replay_policy : ReplayPolicy
          # What percentage of acknowledgements should be samples for observability, 0-100
          getter sample_frequency : String?
          # The rate of message delivery in bits per second
          getter rate_limit_bps : UInt64?
          # The maximum number of messages without acknowledgement that can be outstanding, once this limit is reached message delivery will be suspended
          getter max_ack_pending : Int64?

          @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter inactive_threshold : Time::Span?

          @[JSON::Field(key: "num_replicas")]
          getter replicas : Int32 = 0
          @[JSON::Field(key: "mem_storage")]
          getter? memory_storage : Bool?

          # // Pull based options.
          # MaxRequestBatch    int           `json:"max_batch,omitempty"`
          # MaxRequestExpires  time.Duration `json:"max_expires,omitempty"`
          # MaxRequestMaxBytes int           `json:"max_bytes,omitempty"`
          # The maximum number of messages that can be requested from a pull consumer
          @[JSON::Field(key: "max_batch")]
          getter max_request_batch : Int32?
          # The
          @[JSON::Field(key: "max_expires", converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter max_request_expires : Time::Span?
          @[JSON::Field(key: "max_bytes")]
          getter max_request_max_bytes : Int32?

          def initialize(
            @deliver_subject = nil,
            @durable_name = nil,
            @ack_policy : AckPolicy = :explicit,
            @deliver_policy : DeliverPolicy = :all,
            @replay_policy : ReplayPolicy = :instant,
            @ack_wait = nil,
            @filter_subject = nil,
            max_deliver = nil,
            @opt_start_seq = nil,
            @sample_frequency = nil,
            @opt_start_time = nil,
            @rate_limit_bps = nil,
            max_ack_pending : Int? = nil,
            max_waiting : Int? = nil,
            @idle_heartbeat = nil,
            @flow_control = nil,
            @deliver_group = durable_name,
            @max_request_batch = nil,
            @max_request_expires = nil,
            @max_request_max_bytes = nil,
            @replicas = 0,
            @memory_storage = nil,
            @inactive_threshold = nil
          )
            @max_deliver = max_deliver.try(&.to_i64)
            @max_ack_pending = max_ack_pending.to_i64 if max_ack_pending
            @max_waiting = max_waiting.try(&.to_i64)
          end

          # The way this consumer expects messages to be acknowledged.
          #
          # See [AckPolicy in the NATS server code](https://github.com/nats-io/nats-server/blob/3aa8e63b290ac4ba1c99193827b3f66ad5679904/server/consumer.go#L136-L143)
          enum AckPolicy
            # No acknowledgements are required. All messages are considered
            # acknowledged on delivery.
            None

            # Acknowledging a message acknowledges all messages that came before
            # it.
            All

            # Every message must be acknowledged individually. This is the
            # default.
            Explicit
          end

          # Where to begin consuming messages from a stream.
          #
          # See [DeliverPolicy in the NATS server code](https://github.com/nats-io/nats-server/blob/3aa8e63b290ac4ba1c99193827b3f66ad5679904/server/consumer.go#L105-L120)
          enum DeliverPolicy
            # Deliver _all_ messages from a stream via this consumer
            All

            # Start from the current last message in the stream when this
            # consumer was created
            Last

            # Start _after_ the current last message in the stream when this
            # consumer was created. This is different from `Last` in that it
            # will not begin delivering messages until more are published.
            New

            # Start delivery at the sequence in the stream denoted by
            # `opt_start_seq`. `opt_start_seq` is _required_ when this
            # `DeliverPolicy` is used.
            ByStartSequence

            # Start delivery at the first message whose `timestamp` is equal to
            # or later than `opt_start_time`. `opt_start_time` is _required
            # when this `DeliverPolicy` is used.
            ByStartTime

            # Similar to `Last`, but on a per-subject basis.
            LastPerSubject

            # If you've got this set, something's probably borked. This value
            # only exists in this client because [the server can send it](https://github.com/nats-io/nats-server/blob/3aa8e63b290ac4ba1c99193827b3f66ad5679904/server/consumer.go#L118-L119).
            Undefined
          end

          # A consumer's `replay_policy` is the pace at which to replay messages
          # from a stream.
          #
          # See [ReplayPolicy in the NATS server code](https://github.com/nats-io/nats-server/blob/3aa8e63b290ac4ba1c99193827b3f66ad5679904/server/consumer.go#L157-L162)
          enum ReplayPolicy
            # Tells the NATS server to deliver messages immediately
            Instant

            # Tells the NATS server to deliver messages at the rate they were
            # originally published.
            Original
          end
        end

        struct ConsumerListResponse < Message
          include Enumerable(Consumer)

          getter total : Int64
          getter offset : Int64
          getter limit : Int64
          getter consumers : Array(Consumer)

          def each
            consumers.each { |c| yield c }
          end
        end

        struct ClusterInfo < Message
          getter name : String?
          getter leader : String?
          getter replicas : Array(PeerInfo) { [] of PeerInfo }
        end

        struct PeerInfo < Message
          getter name : String
          getter? current : Bool
          getter? offline : Bool = false
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter active : Time::Span
          getter lag : UInt64?
        end

        module NanosecondsConverter
          def self.to_json(span : Time::Span, json : JSON::Builder)
            json.number span.total_nanoseconds.to_i64
          end

          def self.from_json(json : JSON::PullParser)
            json.read_int.nanoseconds
          end
        end
      end
    end

    class PullSubscription
      getter nats_subscription : ::NATS::Subscription
      getter consumer : API::V1::Consumer
      getter? closed : Bool = false
      @channel : Channel(Message)
      @nats : NATS::Client

      def initialize(@nats_subscription, @consumer, @channel, @nats)
      end

      def fetch(timeout : Time::Span = 2.seconds)
        fetch(1, timeout: timeout).first?
      end

      def fetch(message_count : Int, timeout : Time::Span = 2.seconds, no_wait : Bool = false, max_bytes : Int? = nil) : Enumerable(Message)
        # We have to reimplement request/reply here because we get N replies
        # for 1 request, which NATS request/reply does not support.
        @nats.publish "$JS.API.CONSUMER.MSG.NEXT.#{consumer.stream_name}.#{consumer.name}",
          message: {
            expires: timeout.total_nanoseconds.to_i64,
            batch:   message_count,
          }.to_json,
          reply_to: @nats_subscription.subject

        msgs = Array(Message).new(initial_capacity: message_count)
        total_timeout = timeout
        start = Time.monotonic
        message_count.times do |i|
          select
          when msg = @channel.receive
            msgs << msg
          when timeout(i == 0 ? total_timeout : 100.microseconds)
            break
          end
        end

        msgs
      end

      def ack_next(msg : Message, timeout : Time::Span = 2.seconds, no_wait : Bool = false)
        ack_next(msg, 1, timeout, no_wait).first?
      end

      def ack_next(msg : Message, count : Int, timeout : Time::Span = 2.seconds, no_wait : Bool = false)
        body = String.build do |str|
          str << "+NXT "
          {
            batch:   count,
            no_wait: no_wait,
          }.to_json str
        end

        @nats.publish(msg.reply_to, body, reply_to: @nats_subscription.subject)
        msgs = Array(Message).new(initial_capacity: count)
        start = Time.monotonic
        count.times do |i|
          select
          when msg = @channel.receive
            msgs << msg
          when timeout(timeout - (Time.monotonic - start))
            break
          end
        end
        msgs
      end

      def finalize
        close
      end

      def close
        @nats.unsubscribe @nats_subscription
        @closed = true
      end
    end
  end

  class Client
    # Returns a `NATS::JetStream::Client` that uses this client's connection to
    # the NATS server.
    def jetstream
      @jetstream ||= JetStream::Client.new(self)
    end
  end
end
