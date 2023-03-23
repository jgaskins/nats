require "json"
require "./errors"

module NATS::JetStream
  struct ErrorResponse
    include JSON::Serializable

    getter error : Error

    struct Error
      include JSON::Serializable

      getter code : Int32
      getter err_code : Errors = :none
      getter description : String
    end
  end

  deprecate_api_v1 ErrorResponse
end
