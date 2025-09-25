require "json"

require "./entity"

module NATS::JetStream
  struct StreamGetMsgResponse < Entity
    getter message : Message

    def initialize(*, @message)
    end

    struct Message < Entity
      include JSON::Serializable

      getter subject : String
      getter seq : Int64
      @[JSON::Field(key: "data", converter: ::NATS::JetStream::StreamGetMsgResponse::Message::Base64Data)]
      getter data_string : String
      @[JSON::Field(key: "hdrs", converter: ::NATS::JetStream::StreamGetMsgResponse::Message::HeadersConverter)]
      getter headers : Headers { Headers.new }
      getter time : Time

      def initialize(
        *,
        @subject,
        @seq,
        @data_string,
        @headers,
        @time,
      )
      end

      def data : Bytes
        data_string.to_slice
      end

      module Base64Data
        def self.from_json(json : JSON::PullParser)
          ::Base64.decode_string json.read_string
        end

        def self.to_json(value : Bytes, json : JSON::Builder)
          json.string ::Base64.encode(value)
        end
      end

      module HeadersConverter
        def self.from_json(json : JSON::PullParser)
          if string = json.read_string_or_null
            # Decoded string will be in the format:
            #   "NATS/1.0\r\nHeader1: Value1\r\nHeader2: Value2\r\n\r\n"
            # So we want to omit the first line (preamble) and the last
            # line (it's blank).
            raw = Base64.decode_string(string)
            header_count = raw.count('\n') - 2
            headers = Headers.new

            raw.each_line do |line|
              if separator_index = line.index(':')
                key = line[0...separator_index]
                value = line[separator_index + 2..]
                headers[key] = value
              end
            end

            headers
          end
        end

        def self.to_json(headers : NATS::Headers, json : JSON::Builder)
          json.object do
            headers.each do |(key, value)|
              json.field key, value
            end
          end
        end
      end
    end
  end

  deprecate_api_v1 StreamGetMsgResponse
end
