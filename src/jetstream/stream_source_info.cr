require "./entity"
require "./external_stream"
require "./nanoseconds_converter"
require "./api_error"

module NATS::JetStream
  struct StreamSourceInfo < Entity
    getter name : String
    getter external : ExternalStream?
    @[JSON::Field(converter: ::NATS::JetStream::NanosecondsConverter)]
    getter lag : Time::Span
    @[JSON::Field(converter: ::NATS::JetStream::NanosecondsConverter)]
    getter active : Time::Span
    getter error : APIError?

    def initialize(@name, @external = nil, @lag = 0.seconds, @active = 0.seconds, @error = nil)
    end
  end

  deprecate_api_v1 StreamSourceInfo
end
