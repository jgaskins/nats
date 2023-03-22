require "../nats"
require "./consumer_config"
require "./consumer"
require "./consumer_list_response"

module NATS::JetStream
  # A NATS JetStream consumer is a message index sourced from a stream.
  # It can apply additional filters and records which messages are pending,
  # acknowledged, etc, at the consumer layer.
  struct Consumers
    def initialize(@nats : ::NATS::Client)
    end

    # Create a consumer for the given stream with the given properties,
    # which are passed unmodified to `NATS::JetStream::Consumer.new`.
    def create(
      stream_name : String,
      deliver_policy : ConsumerConfig::DeliverPolicy = :all,
      ack_policy : ConsumerConfig::AckPolicy = :explicit,
      **properties
    ) : Consumer
      consumer_config = ConsumerConfig.new(
        **properties,
        deliver_policy: deliver_policy,
        ack_policy: ack_policy,
      )
      create_consumer = {stream_name: stream_name, config: consumer_config}
      if durable_name = consumer_config.durable_name
        create_consumer_subject = "$JS.API.CONSUMER.DURABLE.CREATE.#{stream_name}.#{durable_name}"
      else
        name = @nats.nuid.next
        create_consumer_subject = "$JS.API.CONSUMER.CREATE.#{stream_name}.#{name}"
      end

      unless response = @nats.request create_consumer_subject, create_consumer.to_json
        raise Error.new("Did not receive a response from NATS JetStream")
      end

      case parsed = (Consumer | ErrorResponse).from_json String.new(response.body)
      in Consumer
        parsed
      in ErrorResponse
        raise Error.new("#{parsed.error.description} (#{parsed.error.code})")
      end
    end

    # Returns a paginated list of consumers for the specified stream.
    def list(stream : JetStream::Stream)
      list stream.config.name
    end

    # Returns a paginated list of consumers for the stream with the
    # specified name.
    def list(stream_name : String)
      if consumers_response = @nats.request "$JS.API.CONSUMER.LIST.#{stream_name}"
        ConsumerListResponse.from_json(String.new(consumers_response.body))
      else
        raise "whoops"
      end
    end

    # Return the consumer with the specified `name` associated with the
    # given stream, yielding to the block if the consumer does not exist
    def info!(stream_name : String, name : String) : Consumer
      info(stream_name, name) do
        raise ArgumentError.new("Consumer #{name.inspect} not found for stream #{stream_name.inspect}")
      end
    end

    def info(stream_name : String, name : String) : Consumer?
      info(stream_name, name) { nil }
    end

    # Return the consumer with the specified `name` associated with the
    # given stream, yielding to the block if the consumer does not exist
    def info(stream_name : String, name : String)
      if consumer_response = @nats.request "$JS.API.CONSUMER.INFO.#{stream_name}.#{name}"
        case parsed = (Consumer | ErrorResponse).from_json(String.new(consumer_response.body))
        in Consumer
          parsed
        in ErrorResponse
          if parsed.error.err_code.consumer_not_found?
            yield
          else
            raise Error.new(parsed.error.description)
          end
        end
      else
        raise "no info for #{name.inspect} (stream #{stream_name.inspect})"
      end
    end

    # Delete the given consumer for the given stream
    def delete(consumer : JetStream::Consumer)
      delete consumer.stream_name, consumer.name
    end

    # Delete the given consumer for the given stream
    def delete(stream : JetStream::Stream, consumer : JetStream::Consumer)
      delete stream.config.name, consumer.name
    end

    # Delete the consumer with the given name associated with the stream
    # with the given name.
    def delete(stream : String, consumer : String)
      @nats.request "$JS.API.CONSUMER.DELETE.#{stream}.#{consumer}"
    end
  end

  deprecate_api_v1 Consumers
end
