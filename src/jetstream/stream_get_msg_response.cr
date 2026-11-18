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
        def self.from_json(json : JSON::PullParser) : String
          ::Base64.decode_string json.read_string
        end

        def self.to_json(value : String | Bytes, json : JSON::Builder) : Nil
          json.string ::Base64.strict_encode(value)
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

        def self.to_json(headers : NATS::Headers, json : JSON::Builder) : Nil
          raw = String.build do |str|
            str << "NATS/1.0\r\n"
            headers.each do |key, values|
              values.each do |value|
                str << key << ": " << value << "\r\n"
              end
            end
            str << "\r\n"
          end
          json.string ::Base64.strict_encode(raw)
        end
      end
    end
  end

  deprecate_api_v1 StreamGetMsgResponse
end
