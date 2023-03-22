require "./entity"
require "./stream_source"

module NATS::JetStream
  struct StreamConfig < Entity
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

    struct Placement < Entity
      getter cluster : String?
      getter tags : Array(String) { %w[] }

      def initialize(@cluster = nil, @tags = nil)
      end
    end

    struct Republish < Entity
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
    @[JSON::Field(converter: ::NATS::JetStream::NanosecondsConverter)]
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
    @[JSON::Field(converter: ::NATS::JetStream::NanosecondsConverter)]
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

  deprecate_api_v1 StreamConfig
end
