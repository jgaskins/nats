require "./entity"
require "./nanoseconds_converter"

module NATS::JetStream
  struct PeerInfo < Entity
    getter name : String
    getter? current : Bool
    getter? offline : Bool = false
    @[JSON::Field(converter: ::NATS::JetStream::NanosecondsConverter)]
    getter active : Time::Span
    getter lag : UInt64?
  end

  deprecate_api_v1 PeerInfo
end
