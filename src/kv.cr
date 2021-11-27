require "json"
require "./nats"
require "./jetstream"

module NATS
  # `NATS::KV` is an experimental interface to the NATS server's experimental KV
  # feature.
  #
  # To use KV:
  #
  # ```
  # require "nats/kv"
  #
  # nats = NATS::Client.new
  # kv = nats.kv
  #
  # bucket = "my-bucket"
  # kv.add(bucket)
  # kv.put bucket, "key", "value"
  #
  # msg = kv.get(bucket, "key").message
  # String.new(msg.data) # => "value"
  # msg.seq              # => 1
  # ```
  module KV
    class Error < ::NATS::Error
    end

    class KeyEmptyError < Error
    end

    class InvalidKeyname < Error
    end

    class Client
      def initialize(@nats : ::NATS::Client)
      end

      # https://github.com/nats-io/nats.go/blob/d7c1d78a50fc9cded3814ae7d7176fa66b73a4b0/kv.go#L295-L307
      def create_bucket(
        bucket : String,
        description : String = "",
        *,
        max_value_size : Int32? = nil,
        history : UInt8? = nil,
        ttl : Time::Span? = nil,
        max_bytes : Int64? = nil,
        storage : JetStream::API::V1::StreamConfig::Storage = :file,
        replicas : Int32? = nil,
        allow_rollup : Bool? = nil,
        deny_delete : Bool? = nil
      )
        @nats.jetstream.stream.create(
          name: "KV_#{bucket}",
          description: description,
          subjects: ["$KV.#{bucket}.>"],
          max_msgs_per_subject: history.to_i64,
          max_bytes: max_bytes,
          max_age: ttl,
          max_msg_size: max_value_size,
          storage: storage,
          replicas: replicas,
          allow_rollup_headers: allow_rollup,
          deny_delete: deny_delete,
                  # No need to ack KV messages
          # no_ack: true,
)
      end

      # Assign `value` to `key` in `bucket`.
      def put(bucket : String, key : String, value : String | Bytes) : Int64
        validate_key! key
        if response = @nats.request("$KV.#{bucket}.#{key}", value)
          PutResponse.from_json(String.new(response.body)).seq
        else
          raise Error.new("No response received from the NATS server when setting #{key.inspect} on KV #{bucket.inspect}")
        end
      end

      struct PutResponse
        include JSON::Serializable
        getter stream : String
        getter seq : Int64
      end

      # Get the value associated with the current
      def get(bucket : String, key : String, revision : Int = 0) : KeyValueEntry?
        validate_key! key unless key == ">"

        if response = @nats.jetstream.stream.get_msg("KV_#{bucket}", last_by_subj: "$KV.#{bucket}.#{key}", sequence: revision)
          operation = KeyValueEntry::Operation::Put

          case response.message.headers.try { |h| h["KV-Operation"]? }
          when "DEL"
            operation = KeyValueEntry::Operation::Delete
          when "PURGE"
            operation = KeyValueEntry::Operation::Purge
          end
          _, bucket_name, key_name = response.message.subject.split('.', 3)
          get = KeyValueEntry.new(
            bucket: bucket_name,
            key: key_name,
            value: response.message.data,
            revision: response.message.seq,
            created_at: response.message.time,
            operation: operation,
          )
        end
      end

      struct KeyValueEntry
        getter bucket : String
        getter key : String
        getter value : Bytes
        getter revision : Int64
        getter created_at : Time
        getter delta : Int64
        getter operation : Operation

        enum Operation
          Put
          Delete
          Purge
        end

        def initialize(@bucket, @key, @value, @revision, @created_at, @operation, @delta = 0i64)
        end
      end

      # Create a key in the given `bucket` with the specified `value`. On
      # success, `create` returns the new revision number for the key. If the
      # key already exists, the value will not be set and `nil` is returned.
      #
      # ```
      # if revision = kv.create("my-bucket", "my-key", "my-value")
      #   # created
      # else
      #   # key already existed and value was not set
      # end
      # ```
      def create(bucket : String, key : String, value : String | Bytes) : Int64?
        update bucket, key, value, revision: 0
      end

      # Update a bucket's key with the specified value only if the current value
      # is the specified revision. If this revision is the latest, the update is
      # not performed and this method returns `nil`.
      #
      # ```
      # if revision = kv.update(bucket, key, value, revision)
      #   # updated
      # else
      #   # outdated revision
      # end
      # ```
      def update(bucket : String, key : String, value : String | Bytes, revision : Int) : Int64?
        validate_key! key

        headers = Headers{
          "Nats-Expected-Last-Subject-Sequence" => revision.to_s,
        }
        if response = @nats.request "$KV.#{bucket}.#{key}", value, headers: headers
          case parsed = (PutResponse | JetStream::API::V1::ErrorResponse).from_json(String.new(response.body))
          in PutResponse
            parsed.seq
          in JetStream::API::V1::ErrorResponse
            # https://github.com/nats-io/nats-server/blob/3f12216fcc349ae0f7af779c6a4647209fbbe9ab/server/errors.json#L62-L71
            if parsed.error.err_code.stream_wrong_last_sequence?
              nil
            else
              raise Error.new(parsed.error.description)
            end
          end
        else
          raise Error.new("No response received from the NATS server when updating #{key.inspect} on KV #{bucket.inspect}")
        end
      end

      #
      def keys(bucket : String)
        keys = Set(String).new

        # Look at all the keys in the current bucket
        watch bucket, ">" do |msg, watch|
          case msg.operation
          when .delete?, .purge?
            keys.delete msg.key
          else
            keys << msg.key
          end
          watch.stop if msg.delta == 0
        end

        keys
      end

      def watch(bucket : String, key : String, *, ignore_deletes = false, &block : KeyValueEntry, Watch ->)
        validate_key! key unless key == ">"

        stop_channel = Channel(Nil).new
        watch = Watch.new(stop_channel)
        inbox = "$WATCH_INBOX.#{Random::Secure.hex}"
        deliver_group = Random::Secure.hex

        stream_name = "KV_#{bucket}"
        consumer = @nats.jetstream.consumer.create(
          stream_name: stream_name,
          deliver_subject: inbox,
          deliver_group: deliver_group,
        )
        subscription = @nats.subscribe inbox, queue_group: deliver_group do |msg|
          js_msg = JetStream::Message.new(msg)
          @nats.jetstream.ack js_msg

          operation = KeyValueEntry::Operation::Put
          case msg.headers.try { |h| h["KV-Operation"]? }
          when "DEL"
            operation = KeyValueEntry::Operation::Delete
          when "PURGE"
            operation = KeyValueEntry::Operation::Purge
          end

          if !ignore_deletes || operation.put?
            _, bucket_name, key_name = msg.subject.split('.', 3)
            get = KeyValueEntry.new(
              bucket: bucket_name,
              key: key_name,
              value: msg.body,
              revision: js_msg.stream_seq,
              created_at: js_msg.timestamp,
              delta: js_msg.pending,
              operation: operation,
            )

            block.call get, watch
          end
        end

        stop_channel.receive
      ensure
        if subscription
          @nats.unsubscribe subscription
        end
        if stream_name && consumer && (name = consumer.name)
          @nats.jetstream.consumer.delete stream_name, name
        end
      end

      class Watch
        def initialize(@stop_channel : Channel(Nil))
        end

        def stop
          @stop_channel.send nil
        end
      end

      def delete(bucket : String, key : String)
        validate_key! key

        headers = Headers{"KV-Operation" => "DEL"}
        if response = @nats.request "$KV.#{bucket}.#{key}", "", headers: headers
          PutResponse.from_json(String.new(response.body)).seq
        else
          raise Error.new("No response received from the NATS server when deleting #{key.inspect} on KV #{bucket.inspect}")
        end
      end

      def purge(bucket : String, key : String)
        headers = Headers{"KV-Operation" => "PURGE"}
        if response = @nats.request "$KV.#{bucket}.#{key}", "", headers: headers
          PutResponse.from_json(String.new(response.body)).seq
        else
          raise Error.new("No response received from the NATS server when purging #{key.inspect} on KV #{bucket.inspect}")
        end
      end

      def delete_bucket(bucket : String)
        @nats.jetstream.stream.delete "KV_#{bucket}"
      end

      private def validate_key!(key : String)
        if key.empty?
          raise KeyEmptyError.new("Key name cannot be empty")
        end
        if key.starts_with?('.') || key.ends_with?('.')
          raise InvalidKeyname.new("Key cannot start or end with a `.` - got #{key.inspect}")
        end
        if key !~ %r{\A[-/_=\.a-zA-Z0-9]+\z}
          raise InvalidKeyname.new("Key may only contain alphanumeric characters, -, /, _, =, and . (got: #{key.inspect})")
        end
      end
    end
  end

  class Client
    # Returns a `NATS::KV::Client` that uses this client's connection to
    # the NATS server.
    def kv
      @kv ||= KV::Client.new(self)
    end
  end
end
