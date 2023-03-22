require "./entity"

module NATS::JetStream
  struct StreamState < Entity
    getter messages : Int64
    getter bytes : Int64
    getter first_seq : Int64
    getter first_ts : Time
    getter last_seq : Int64
    getter last_ts : Time
    getter consumer_count : Int32
  end

  deprecate_api_v1 StreamState
end
