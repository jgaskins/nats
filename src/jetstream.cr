require "json"
require "./nats"

module NATS
  module JetStream
    class Error < ::NATS::Error
    end

    class Client
      def initialize(@nats : ::NATS::Client)
      end

      def stream
        API::Stream.new(@nats)
      end

      def consumer
        API::Consumer.new(@nats)
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
              NATS::JetStream::API::V1::Stream.from_json(response.body_io)
            else
              raise "whoops"
            end
          end

          def delete(stream : JetStream::API::V1::Stream)
            @nats.request "$JS.API.STREAM.DELETE.#{stream.config.name}"
          end
        end

        struct Consumer
          def initialize(@nats : ::NATS::Client)
          end

          def create(stream_name : String, **kwargs) : JetStream::API::V1::Consumer
            consumer_config = NATS::JetStream::API::V1::ConsumerConfig.new(**kwargs)
            create_consumer = { stream_name: stream_name, config: consumer_config }
            create_consumer_subject = "$JS.API.CONSUMER.DURABLE.CREATE.#{stream_name}.#{consumer_config.durable_name}"

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
            @nats.request "$JS.API.CONSUMER.DELETE.#{stream.config.name}.#{consumer.name}"
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
      getter reply_to : String?

      # <stream>.<consumer>.<delivered count>.<stream sequence>.<consumer sequence>.<timestamp>.<pending messages>
      def self.from_nats_message(msg : ::NATS::Message)
        if reply_to = msg.reply_to
          _jetstream, _ack, stream, consumer, delivered_count, stream_seq, consumer_seq, timestamp, pending_messages = reply_to.split(".")
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
            reply_to: msg.reply_to,
          )
        else
          raise InvalidNATSMessage.new("Message does not have a reply_to set")
        end
      end

      def initialize(@stream, @consumer, @delivered_count, @stream_seq, @consumer_seq, @timestamp, @pending, @body, @subject, @reply_to)
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
        end

        struct Consumer < Message
          getter stream_name : String?
          getter name : String
          getter created : Time
          getter config : ConsumerConfig
          getter delivered : Sequence
          getter ack_floor : Sequence
          getter num_ack_pending : Int64
          getter num_redelivered : Int64
          getter num_waiting : Int64
          getter num_pending : Int64

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

            def self.from_json(json : JSON::PullParser) : self
              case value = json.read_string
              when "memory" then Memory
              when "file" then File
              else
                raise "Invalid storage type: #{value}"
              end
            end

            def self.to_json(value : self, json : JSON::Builder)
              json.string value.to_s.downcase
            end
          end

          getter name : String
          getter subjects : Array(String)
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::StreamConfig::Storage)]
          getter storage : Storage
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::MicrosecondsConverter)]
          getter max_age : Time::Span?
          getter max_bytes : Int64?
          getter max_msg_size : Int32?
          getter max_msgs : Int64?
          getter max_consumers : Int32?
          getter? no_ack : Bool?
          getter replicas : Int32?
          getter retention : String?
          getter discard : String?

          # Not sure what this does
          # @[JSON::Field(key: "Duplicates")]
          # getter duplicates : String?

          def initialize(@name, @subjects, @max_age = nil, @max_bytes = nil, @max_msg_size = nil, @max_msgs = nil, @max_consumers = nil, @no_ack = false, @replicas = nil, @retention = nil, @discard = nil, @storage : Storage = :file)
          end
        end

        struct ConsumerConfig < Message
          # AckPolicy	How messages should be acknowledged, AckNone, AckAll or AckExplicit
          getter ack_policy : String
          # AckWait	How long to allow messages to remain un-acknowledged before attempting redelivery
          @[JSON::Field(converter: ::NATS::JetStream::API::V1::NanosecondsConverter)]
          getter ack_wait : Time::Span?
          # DeliverPolicy	The initial starting mode of the consumer, DeliverAll, DeliverLast, DeliverNew, DeliverByStartSequence or DeliverByStartTime
          getter deliver_policy : String
          # DeliverySubject	The subject to deliver observed messages, when not set, a pull-based Consumer is created
          getter deliver_subject : String?
          # Durable	The name of the Consumer
          getter durable_name : String?
          # FilterSubject	When consuming from a Stream with many subjects, or wildcards, select only a specific incoming subjects, supports wildcards
          getter filter_subject : String?
          # MaxDeliver	Maximum amount times a specific message will be delivered. Use this to avoid poison pills crashing all your services forever
          getter max_deliver : Int64?
          # OptStartSeq	When first consuming messages from the Stream start at this particular message in the set
          getter opt_start_seq : Int64?
          # ReplayPolicy	How messages are sent ReplayInstant or ReplayOriginal
          getter replay_policy : String
          # SampleFrequency	What percentage of acknowledgements should be samples for observability, 0-100
          getter sample_frequency : Int8?
          # OptStartTime	When first consuming messages from the Stream start with messages on or after this time
          getter opt_start_time : Time?
          # RateLimit	The rate of message delivery in bits per second
          getter rate_limit : Int64?
          # MaxAckPending	The maximum number of messages without acknowledgement that can be outstanding, once this limit is reached message delivery will be suspended
          getter max_ack_pending : Int64?

          def initialize(@deliver_subject = nil, @durable_name = nil, @ack_policy = "explicit", @deliver_policy = "all", @replay_policy = "instant", @ack_wait = nil, @filter_subject = nil, @max_deliver = nil, @opt_start_seq = nil, @sample_frequency = nil, @opt_start_time = nil, @rate_limit = nil, max_ack_pending : Int? = nil)
            @max_ack_pending = max_ack_pending.to_i64 if max_ack_pending
          end
          # getter name : String
          # getter stream : String
          # getter delivery_target : String?
          # getter start_policy : String?
          # getter acknowledgement_policy : String?
          # getter replay_policy : String?
          # getter stream_filter : String?
          # getter max_deliveries : Int64?

          # def initialize(@name, @stream, @delivery_target = nil, @start_policy = nil, @acknowledgement_policy = nil, @replay_policy = nil, @stream_filter = nil, @max_deliveries = nil)
          # end
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
