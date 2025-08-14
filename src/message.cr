require "http/headers"

module NATS
  struct Message
    getter subject : String
    getter reply_to : String?
    getter headers : Headers { Headers.new }
    getter data_string : String

    alias Headers = HTTP::Headers

    def initialize(@subject, data @data_string, @reply_to = nil, @headers = nil)
    end

    def data
      data_string.to_slice
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
