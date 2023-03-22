require "./entity"

module NATS::JetStream
  struct APIError < Entity
    getter code : Int64
    getter err_code : UInt16?
    getter description : String?
  end

  deprecate_api_v1 APIError
end
