module NATS::JetStream
  abstract struct Entity
    include JSON::Serializable
  end

  macro deprecate_api_v1(name)
    module ::NATS::JetStream::API::V1
      @[Deprecated("The API::V1 namespace was unnecessary, use NATS::JetStream::{{name}} instead")]
      alias {{name}} = JetStream::{{name}}
    end
  end
end
