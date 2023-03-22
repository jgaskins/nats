require "json"
require "./entity"

module NATS::JetStream
  struct ExternalStream < Entity
    @[JSON::Field(key: "api")]
    getter api_prefix : String
    @[JSON::Field(key: "deliver")]
    getter deliver_prefix : String

    def initialize(*, @api_prefix = "", @deliver_prefix = "")
    end
  end

  deprecate_api_v1 ExternalStream
end
