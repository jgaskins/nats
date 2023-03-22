require "./entity"
require "./consumer_config"
require "./cluster_info"

module NATS::JetStream
  struct Consumer < Entity
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
    struct Sequence < Entity
      getter consumer_seq : Int64
      getter stream_seq : Int64
    end
  end

  deprecate_api_v1 Consumer
end
