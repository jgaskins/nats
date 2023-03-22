require "json"

require "../nats"
require "./stream"
require "./stream_config"
require "./stream_list_response"
require "./stream_get_msg_response"
require "./error_response"

module NATS::JetStream
  # A stream in NATS JetStream represents the history of messages
  # pertaining to a given domain. When you publish a message to a subject
  # that a stream is monitoring, the stream then adds that message to its
  # history in the order it was published.
  struct Streams
    def initialize(@nats : ::NATS::Client)
    end

    # Create a stream of the given storage type and with the given
    # properties, which are passed unmodified to
    # `NATS::JetStream::StreamConfig.new`.
    def create(
      storage : StreamConfig::Storage,
      retention : StreamConfig::RetentionPolicy? = nil,
      discard : StreamConfig::DiscardPolicy? = nil,
      **kwargs
    ) : Stream
      create_stream = StreamConfig.new(
        **kwargs,
        storage: storage,
        retention: retention,
        discard: discard,
      )

      if create_stream.name.includes? '.'
        raise Error.new("Cannot create stream with '.' in the name")
      end

      if response = @nats.request "$JS.API.STREAM.CREATE.#{create_stream.name}", create_stream.to_json
        case parsed = (Stream | ErrorResponse).from_json String.new(response.body)
        in ErrorResponse
          raise Error.new("#{parsed.error.description} (#{parsed.error.code})")
        in Stream
          parsed
        end
      else
        raise Error.new("Did not receive a response from NATS JetStream")
      end
    rescue ex
      raise ex
    end

    # List all available streams
    def list(subject : String? = nil, offset : Int? = nil, limit : Int? = nil)
      body = {subject: subject, offset: offset, limit: limit}.to_json

      if response = @nats.request "$JS.API.STREAM.LIST", body
        StreamListResponse.new @nats.jetstream,
          json: JSON::PullParser.new(String.new(response.body)),
          subject: subject
      else
        raise "whoops"
      end
    end

    # Get the current state of the stream with the given `name`
    def info(name : String) : Stream?
      if response = @nats.request "$JS.API.STREAM.INFO.#{name}"
        case parsed = (Stream | ErrorResponse).from_json(String.new(response.body))
        in Stream
          parsed
        in ErrorResponse
          if parsed.error.err_code.stream_not_found?
            nil
          else
            raise Error.new(parsed.error.description)
          end
        end
      else
        raise Error.new("Response timed out while fetching stream #{name.inspect}")
      end
    end

    # Delete the given stream
    def delete(stream : JetStream::Stream)
      delete stream.config.name
    end

    # Delete the stream with the given name
    def delete(stream : String)
      @nats.request "$JS.API.STREAM.DELETE.#{stream}"
    end

    def get_msg(stream : String, *, last_by_subject : String)
      get_msg stream, {last_by_subj: last_by_subject}
    end

    def get_msg(stream : String, *, sequence : Int, next_by_subject : String? = nil)
      get_msg stream, {seq: sequence, next_by_subj: next_by_subject}
    end

    protected def get_msg(stream : String, params)
      if response = @nats.request "$JS.API.STREAM.MSG.GET.#{stream}", params.to_json
        case parsed = (StreamGetMsgResponse | ErrorResponse).from_json String.new(response.body)
        in StreamGetMsgResponse
          parsed
        in ErrorResponse
          if parsed.error.err_code.no_message_found?
            nil # No message
          else
            raise Error.new(parsed.error.description)
          end
        end
      else
        raise Error.new("Did not receive a response when getting message from stream #{stream.inspect} with options #{params}")
      end
    end

    def purge(stream : String, subject : String) : Int64
      if response = @nats.request("$JS.API.STREAM.PURGE.#{stream}", {filter: subject}.to_json)
        case parsed = (PurgeStreamResponse | ErrorResponse).from_json String.new(response.body)
        in PurgeStreamResponse
          parsed.purged
        in ErrorResponse
          raise Error.new(parsed.error.description)
        end
      else
        raise Error.new("Did not receive a response when purging stream #{stream.inspect} of subject #{subject.inspect}")
      end
    end

    struct PurgeStreamResponse
      include JSON::Serializable

      getter purged : Int64
    end
  end

  deprecate_api_v1 Streams
end
