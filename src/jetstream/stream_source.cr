require "./entity"
require "./external_stream"
require "./subject_transform"

module NATS::JetStream
  struct StreamSource < Entity
    getter name : String
    getter opt_start_seq : UInt64?
    getter opt_start_time : Time?
    getter filter_subject : String?
    getter subject_transforms : Array(SubjectTransform)?
    getter external : ExternalStream?
    getter consumer : Consumer?

    def initialize(
      @name,
      @opt_start_seq = nil,
      @opt_start_time = nil,
      @filter_subject = nil,
      @subject_transforms = nil,
      @external = nil,
      @consumer = nil,
    )
    end

    # The consumer the server uses to read from the source stream
    struct Consumer < Entity
      getter name : String?
      getter deliver_subject : String?

      def initialize(*, @name = nil, @deliver_subject = nil)
      end
    end
  end

  deprecate_api_v1 StreamSource
end
