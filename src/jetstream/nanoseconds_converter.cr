require "json"

module NATS::JetStream
  module NanosecondsConverter
    def self.to_json(span : Time::Span, json : JSON::Builder)
      json.number span.total_nanoseconds.to_i64
    end

    def self.from_json(json : JSON::PullParser)
      json.read_int.nanoseconds
    end
  end

  deprecate_api_v1 NanosecondsConverter
end
