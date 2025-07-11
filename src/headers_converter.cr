require "json/pull_parser"

require "./headers"

module NATS::HeadersConverter
  extend self

  def from_json(json : JSON::PullParser) : Headers
    Headers.new.merge! Hash(String, String | Array(String)).new(json)
  end

  def to_json(headers : Headers, json : JSON::Builder) : Nil
    json.object do
      headers.each do |key, values|
        json.field key do
          json.array do
            values.each do |value|
              json.scalar value
            end
          end
        end
      end
    end
  end
end
