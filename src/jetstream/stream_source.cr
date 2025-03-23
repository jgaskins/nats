require "./entity"
require "./external_stream"

module NATS::JetStream
  struct StreamSource < Entity
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
      @external = nil,
    )
    end
  end

  deprecate_api_v1 StreamSource
end
