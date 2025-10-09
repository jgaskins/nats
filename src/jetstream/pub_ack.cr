require "json"
require "./entity"

module NATS::JetStream
  struct PubAck < Entity
    getter stream : String
    @[JSON::Field(key: "seq")]
    getter sequence : Int64
    getter? duplicate : Bool = false
    getter domain : String?
    getter val : String?
  end

  deprecate_api_v1 PubAck
end
