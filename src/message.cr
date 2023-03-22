module NATS
  struct Message
    getter subject : String
    getter body : Bytes
    getter reply_to : String?
    getter headers : Headers?

    alias Headers = Hash(String, String)

    def initialize(@subject, @body, @reply_to = nil, @headers = nil)
    end

    @[Deprecated("Instantiating a new IO::Memory for each message made them heavier than intended, so we're now recommending using `String.new(msg.body)`")]
    def body_io
      @body_io ||= IO::Memory.new(@body)
    end
  end
end
