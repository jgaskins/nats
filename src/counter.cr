require "big"
require "big/json"

require "./nats"
require "./jetstream"

module NATS
  struct Counter
    def initialize(@nats : Client)
    end

    def set(subject : String, to value : Int) : Nil
      reset subject
      increment subject, by: value
    end

    def increment(subject : String, by delta : Int = 1) : BigInt
      response = @nats.request subject, headers: Headers{"Nats-Incr" => delta.to_s}

      unless response
        raise MissingCounter.new("No response from NATS, missing counter for subject: #{subject.inspect}")
      end

      pub_ack = JetStream::PubAck.from_json(response.data_string)

      if val = pub_ack.val
        val.to_big_i
      else
        raise Error.new("Missing value from NATS server response: #{response.data_string}")
      end
    end

    def decrement(subject, by delta : Int = 1) : BigInt
      increment subject, by: -delta
    end

    def increment!(subject : String, by delta : Int = 1) : Nil
      @nats.publish subject, headers: Headers{"Nats-Incr" => delta.to_s}
    end

    def decrement!(subject : String, by delta : Int = 1) : Nil
      @nats.publish subject, headers: Headers{"Nats-Incr" => (-delta).to_s}
    end

    def get(subject : String) : BigInt
      if stream = @nats.jetstream.stream.list(subject: subject).first?
        unless response = @nats.jetstream.stream.get_msg(stream.config.name, last_by_subject: subject)
          # There are is no message in the stream on that subject so, by
          # definition, the counter is 0.
          return 0.to_big_i
        end

        result = GetResponse.from_json(response.message.data_string)
        result.value
      else
        raise MissingCounter.new("No counter for subject: #{subject.inspect}")
      end
    end

    def reset(subject : String) : Nil
      # The counter could be fed by other streams, so we get the current value
      # and decrement by that before purging.
      # See https://github.com/nats-io/nats-architecture-and-design/blob/3994269ce71bb4dd69c8080b5fff6b4911b410c8/adr/ADR-49.md
      current_value = get(subject)
      decrement subject, by: current_value

      stream = @nats.jetstream.stream.list(subject: subject).first
      @nats.jetstream.stream.purge stream.config.name, subject
    end

    def create(name : String, subjects : Array(String), storage : NATS::JetStream::StreamConfig::Storage = :file)
      stream = @nats.jetstream.stream.create(
        name: name,
        subjects: subjects,
        storage: storage,
        allow_msg_counter: true,
        allow_direct: true,
      )

      Counter.new(@nats)
    end

    private struct GetResponse
      include JSON::Serializable

      @[JSON::Field(key: "val")]
      getter value : BigInt
    end

    class Error < NATS::Error
    end

    class MissingCounter < Error
    end
  end

  class Client
    def counter
      Counter.new(self)
    end
  end
end
