require "json"

module NATS::JetStream
  enum Errors : Int32
    # https://github.com/nats-io/nats-server/blob/main/server/errors.json

    None                              =     0
    ConsumerNotFound                  = 10014
    NoMessageFound                    = 10037
    StreamNotFound                    = 10059
    StreamWrongLastSequence           = 10071
    MaximumMessagesPerSubjectExceeded = 10077

    def self.new(json : JSON::PullParser)
      new json.read_int.to_i
    end
  end

  class Error < NATS::Error
    getter jetstream_message : Message?

    def initialize(
      message : String?,
      @jetstream_message = nil,
      cause : Exception? = nil,
    )
      super message, cause: cause
    end
  end
end
