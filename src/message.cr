require "http/headers"

module NATS
  struct Message
    getter subject : String
    getter data : Bytes
    getter reply_to : String?
    getter headers : Headers { Headers.new }
    getter data_string : String { String.new data }

    alias Headers = HTTP::Headers

    def initialize(@subject, @data, @reply_to = nil, @headers = nil)
    end

    @[Deprecated("Instantiating a new IO::Memory for each message made them heavier than intended, so we're now recommending using `String.new(msg.body)`")]
    def body_io
      @body_io ||= IO::Memory.new(@body)
    end

    def body
      data
    end
  end
end
