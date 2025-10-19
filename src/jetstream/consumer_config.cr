require "./entity"

module NATS::JetStream
  struct ConsumerConfig < Entity
    # How messages should be acknowledged: none, all, or explicit
    getter ack_policy : AckPolicy
    # How long to allow messages to remain un-acknowledged before attempting redelivery
    @[JSON::Field(converter: ::NATS::JetStream::NanosecondsConverter)]
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
    @[JSON::Field(converter: ::NATS::JetStream::NanosecondsConverter)]
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

    @[JSON::Field(converter: ::NATS::JetStream::NanosecondsConverter)]
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
    @[JSON::Field(key: "max_expires", converter: ::NATS::JetStream::NanosecondsConverter)]
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
      @headers_only = nil,
      @deliver_group = durable_name,
      @max_request_batch = nil,
      @max_request_expires = nil,
      @max_request_max_bytes = nil,
      @replicas = 0,
      @memory_storage = nil,
      @inactive_threshold = nil,
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

  deprecate_api_v1 ConsumerConfig
end
