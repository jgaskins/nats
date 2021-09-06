require "json"
require "./nats"
require "./error"

module NATS
  # NATS JetStream
  module JetStream
    class Error < ::NATS::Error
    end

    # This class provides a client for NATS JetStream for at-least-once delivery.
    # You can either instantiate it with a NATS client or with the
    # `NATS::Client#jetstream` method as a shortcut.
    class Client
      def initialize(@nats : ::NATS::Client)
      end

      def stream
        API::Stream.new(@nats)
      end

      def consumer
        API::Consumer.new(@nats)
      end

      def subscribe(subject : String, queue_group : String? = nil, &block : Message ->)
        @nats.subscribe subject, queue_group: queue_group do |msg|
          block.call Message.new(msg)
        end
      end

      def ack(msg : Message)
        @nats.publish msg.reply_to, "+ACK"
      end

      def nack(msg : Message)
        @nats.publish msg.reply_to, "-NAK"
      end

      module API
        struct ErrorResponse
          include JSON::Serializable

          getter error : Error

          struct Error
            include JSON::Serializable

            getter code : Int32
            getter description : String
          end
        end

        struct Stream
          def initialize(@nats : ::NATS::Client)
          end

          def create(storage : JetStream::API::V1::StreamConfig::Storage, **kwargs)
            create_stream = JetStream::API::V1::StreamConfig.new(**kwargs, storage: storage)
            if response = @nats.request "$JS.API.STREAM.CREATE.#{create_stream.name}", create_stream.to_json
              case parsed = (JetStream::API::V1::Stream | ErrorResponse).from_json response.body_io
              when ErrorResponse
                raise JetStream::Error.new("#{parsed.error.description} (#{parsed.error.code})")
              else
                parsed
              end
            else
              raise JetStream::Error.new("Did not receive a response from NATS JetStream")
            end
          rescue ex
            pp body: String.new(response.try(&.body) || Bytes.empty), parsed: parsed
            raise ex
          end

          def list
            if response = @nats.request "$JS.API.STREAM.LIST"
              NATS::JetStream::API::V1::StreamListResponse.from_json(response.body_io)
            else
              raise "whoops"
            end
          end

          def info(name : String)
            if response = @nats.request "$JS.API.STREAM.INFO.#{name}"
              NATS::JetStream::API::V1::Stream.from_json(String.new(response.body))
            else
              raise "whoops"
            end
          end

          def delete(stream : JetStream::API::V1::Stream)
            delete stream.config.name
          end

          def delete(stream : String)
            @nats.request "$JS.API.STREAM.DELETE.#{stream}"
          end
        end

        struct Consumer
          def initialize(@nats : ::NATS::Client)
          end

          def create(stream_name : String, **kwargs) : JetStream::API::V1::Consumer
            consumer_config = NATS::JetStream::API::V1::ConsumerConfig.new(**kwargs)
            create_consumer = {stream_name: stream_name, config: consumer_config}
            if durable_name = consumer_config.durable_name
              create_consumer_subject = "$JS.API.CONSUMER.DURABLE.CREATE.#{stream_name}.#{durable_name}"
            else
              create_consumer_subject = "$JS.API.CONSUMER.CREATE.#{stream_name}"
            end

            unless response = @nats.request create_consumer_subject, create_consumer.to_json
              raise JetStream::Error.new("Did not receive a response from NATS JetStream")
            end

            case parsed = (JetStream::API::V1::Consumer | ErrorResponse).from_json response.body_io
            when ErrorResponse
              raise JetStream::Error.new("#{parsed.error.description} (#{parsed.error.code})")
            else
              parsed
            end
          end

          def list(stream : JetStream::API::V1::Stream)
            list stream.config.name
          end

          def list(stream_name : String)
            if consumers_response = @nats.request "$JS.API.CONSUMER.LIST.#{stream_name}"
              NATS::JetStream::API::V1::ConsumerListResponse.from_json(consumers_response.body_io)
            else
              raise "whoops"
            end
          end

          def info(stream_name : String, name : String)
            if consumer_response = @nats.request "$JS.API.CONSUMER.INFO.#{stream_name}.#{name}"
              NATS::JetStream::API::V1::Consumer.from_json(consumer_response.body_io)
            else
              raise "no info for #{name.inspect} (stream #{stream_name.inspect})"
            end
          end

          def delete(stream : JetStream::API::V1::Stream, consumer : JetStream::API::V1::Consumer)
            delete stream.config.name, consumer.name
          end

          def delete(stream : String, consumer : String)
            @nats.request "$JS.API.CONSUMER.DELETE.#{stream}.#{consumer}"
          end
        end
      end
    end

    struct Message
      getter stream : String
      getter consumer : String
      getter delivered_count : Int64
      getter stream_seq : Int64
      getter consumer_seq : Int64
      getter timestamp : Time
      getter pending : Int64
      getter body : Bytes
      getter subject : String
      getter reply_to : String
      getter headers : ::NATS::Message::Headers?

      def self.new(msg : ::NATS::Message)
        # reply_to format:
        # $JS.ACK.<stream>.<consumer>.<delivered count>.<stream sequence>.<consumer sequence>.<timestamp>.<pending messages>
        if reply_to = msg.reply_to
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
          raise InvalidNATSMessage.new("Message does not have a reply_to set")
        end
      end

      def initialize(@stream, @consumer, @delivered_count, @stream_seq, @consumer_seq, @timestamp, @pending, @body, @subject, @reply_to, @headers)
      end

      class InvalidNATSMessage < Exception
      end
    end

    module API
      abstract struct Message
        include JSON::Serializable
      end

      module V1
        struct Stream < Message
          getter config : StreamConfig
          getter created : Time
          getter state : StreamState
          getter cluster : ClusterInfo?
          getter mirror : StreamSourceInfo?
          getter sources : Array(StreamSourceInfo) = [] of StreamSourceInfo
        end

        struct StreamSourceInfo < Message
          getter name : String
          getter external : ExternalStream?
          getter lag : UInt64
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter active : Time::Span
          getter error : APIError?
        end

        struct ExternalStream < Message
          getter api : String
          getter deliver : String
        end

        struct APIError < Message
          getter code : Int64
          getter err_code : UInt16?
          getter description : String?
        end

        struct Consumer < Message
          getter stream_name : String?
          getter name : String?
          getter created : Time
          getter config : ConsumerConfig
          getter delivered : Sequence
          getter ack_floor : Sequence
          getter num_ack_pending : Int64
          getter num_redelivered : Int64
          getter num_waiting : Int64
          getter num_pending : Int64
          getter cluster : ClusterInfo?
          getter? push_bound : Bool = false

          struct Sequence < Message
            getter consumer_seq : Int64
            getter stream_seq : Int64
          end
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
          enum Storage
            Memory
            File
          end

          enum RetentionPolicy
            Limits
            Interest
            Workqueue
          end

          enum DiscardPolicy
            Old
            New
          end

          struct Placement < Message
            getter cluster : String
            getter tags : Array(String) = %w[]
          end

          getter name : String
          getter subjects : Array(String)
          getter storage : Storage
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::MicrosecondsConverter)]
          getter max_age : Time::Span?
          getter max_bytes : Int64?
          getter max_msg_size : Int32?
          getter max_msgs : Int64?
          getter max_consumers : Int32?
          getter? no_ack : Bool?
          getter replicas : Int32?
          getter retention : RetentionPolicy?
          getter discard : DiscardPolicy?
          # @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          # getter duplicate_window : Time::Span?
          getter placement : Placement?
          getter mirror : StreamSourceInfo?
          getter sources : Array(StreamSourceInfo) = [] of StreamSourceInfo

          def initialize(
            @name,
            @subjects,
            @max_age = nil,
            @max_bytes = nil,
            @max_msg_size = nil,
            @max_msgs = nil,
            @max_consumers = nil,
            @no_ack = false,
            @replicas = nil,
            @retention : RetentionPolicy? = nil,
            @discard : DiscardPolicy? = nil,
            @storage : Storage = :file,
          )
          end
        end

        struct ConsumerConfig < Message
          # AckPolicy	How messages should be acknowledged, AckNone, AckAll or AckExplicit
          getter ack_policy : AckPolicy
          # AckWait	How long to allow messages to remain un-acknowledged before attempting redelivery
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter ack_wait : Time::Span?
          # DeliverPolicy	The initial starting mode of the consumer, DeliverAll, DeliverLast, DeliverNew, DeliverByStartSequence or DeliverByStartTime
          getter deliver_policy : DeliverPolicy
          # DeliverySubject	The subject to deliver observed messages, when not set, a pull-based Consumer is created
          getter deliver_subject : String?
          # Durable	The name of the Consumer
          getter durable_name : String?

          getter deliver_group : String?
          getter description : String?
          getter max_waiting : Int64?
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter idle_heartbeat : Time::Span?
          getter? flow_control : Bool = false

          # Apparently we're not supposed to use this in clients
          # See https://github.com/nats-io/nats-server/blob/3aa8e63b290ac4ba1c99193827b3f66ad5679904/server/consumer.go#L70-L71
          # getter? direct : Bool = false


          # FilterSubject	When consuming from a Stream with many subjects, or wildcards, select only a specific incoming subjects, supports wildcards
          getter filter_subject : String?
          # MaxDeliver	Maximum amount times a specific message will be delivered. Use this to avoid poison pills crashing all your services forever
          getter max_deliver : Int64?
          # OptStartSeq	When first consuming messages from the Stream start at this particular message in the set
          getter opt_start_seq : Int64?
          # ReplayPolicy	How messages are sent ReplayInstant or ReplayOriginal
          getter replay_policy : ReplayPolicy
          # SampleFrequency	What percentage of acknowledgements should be samples for observability, 0-100
          getter sample_frequency : String?
          # OptStartTime	When first consuming messages from the Stream start with messages on or after this time
          getter opt_start_time : Time?
          # RateLimit	The rate of message delivery in bits per second
          getter rate_limit_bps : UInt64?
          # MaxAckPending	The maximum number of messages without acknowledgement that can be outstanding, once this limit is reached message delivery will be suspended
          getter max_ack_pending : Int64?

          def initialize(@deliver_subject = nil, @durable_name = nil, @ack_policy : AckPolicy = :explicit, @deliver_policy : DeliverPolicy = :all, @replay_policy : ReplayPolicy = :instant, @ack_wait = nil, @filter_subject = nil, max_deliver = nil, @opt_start_seq = nil, @sample_frequency = nil, @opt_start_time = nil, @rate_limit_bps = nil, max_ack_pending : Int? = nil, @idle_heartbeat = nil, @deliver_group = durable_name)
            @max_deliver = max_deliver.try(&.to_i64)
            @max_ack_pending = max_ack_pending.to_i64 if max_ack_pending
          end

          enum AckPolicy
            # See https://github.com/nats-io/nats-server/blob/3aa8e63b290ac4ba1c99193827b3f66ad5679904/server/consumer.go#L136-L143
            None
            All
            Explicit
          end

          enum DeliverPolicy
            # See https://github.com/nats-io/nats-server/blob/3aa8e63b290ac4ba1c99193827b3f66ad5679904/server/consumer.go#L105-L120
            All
            Last
            New
            ByStartSequence
            ByStartTime
            LastPerSubject
            Undefined
          end

          enum ReplayPolicy
            # See https://github.com/nats-io/nats-server/blob/3aa8e63b290ac4ba1c99193827b3f66ad5679904/server/consumer.go#L157-L162
            Instant
            Original
          end
        end

        struct ClusterInfo < Message
          getter name : String?
          getter leader : String?
          getter replicas : Array(PeerInfo) = [] of PeerInfo
        end

        struct PeerInfo < Message
          getter name : String
          getter? current : Bool
          getter? offline : Bool
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter active : Time::Span
          getter lag : UInt64
        end

        module MicrosecondsConverter
          def self.to_json(span : Time::Span, json : JSON::Builder)
            json.number span.total_microseconds.to_i64
          end

          def self.from_json(json : JSON::PullParser)
            json.read_int.microseconds
          end
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
  end
end
