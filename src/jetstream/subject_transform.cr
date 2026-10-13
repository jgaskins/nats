require "./entity"

module NATS::JetStream
  # Transforms the subjects of messages matching `source` into `destination`
  # as they're stored in a stream
  struct SubjectTransform < Entity
    @[JSON::Field(key: "src")]
    getter source : String
    @[JSON::Field(key: "dest")]
    getter destination : String

    def initialize(*, @source, @destination)
    end
  end
end
