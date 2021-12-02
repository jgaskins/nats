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

    class KeyError < Error
    end

    class InvalidKeyname < Error
    end

    struct Bucket
      getter name : String
      getter stream_name : String
      @description : String?
      @max_value_size : Int32?
      @history : Int64?
      @ttl : Time::Span?
      @max_bytes : Int64?
      @storage : JetStream::API::V1::StreamConfig::Storage
      @replicas : Int32?
      @allow_rollup : Bool?
      @deny_delete : Bool?

      # :nodoc:
      def self.new(stream : JetStream::API::V1::Stream, kv : Client)
        config = stream.config
        new(
          name: config.name.lchop("KV_"),
          stream_name: config.name,
          description: config.description,
          max_value_size: config.max_msg_size,
          history: config.max_msgs_per_subject,
          ttl: config.max_age,
          max_bytes: config.max_bytes,
          storage: config.storage,
          replicas: config.replicas,
          allow_rollup: config.allow_rollup_headers?,
          deny_delete: config.deny_delete?,
          kv: kv,
        )
      end

      # :nodoc:
      def initialize(
        @name : String,
        @stream_name : String,
        @description : String?,
        @max_value_size : Int32?,
        @history : Int64?,
        @ttl : Time::Span?,
        @max_bytes : Int64?,
        @storage : JetStream::API::V1::StreamConfig::Storage,
        @replicas : Int32?,
        @allow_rollup : Bool?,
        @deny_delete : Bool?,
        @kv : Client
      )
      end

      # Set the value of a key
      def put(key : String, value : Data)
        @kv.put name, key, value
      end

      def set(key : String, value : Data)
        @kv.set name, key, value
      end

      # Set the value of a key
      def []=(key : String, value : Data)
        put key, value
      end

      # Get the entry for a key as a `KV::Entry` - returns `nil` if the key does
      # not exist or if it's been deleted with `ignore_deletes` set to `true`.
      #
      # *Important*: If you do not set `ignore_deletes`, you may get a deleted
      # key. This is because the keys are stored in a stream and deleting the
      # key sets an `operation` flag (implemented in the NATS server as a
      # `KV-Operation` message header) and this method retrieves the last entry
      # in the stream for this key. `ignore_deletes` simply tells the client to
      # ignore deleted messages.
      def get(key : String, *, ignore_deletes = false) : Entry?
        @kv.get name, key, ignore_deletes: ignore_deletes
      end

      # Get the value of a key, if it exists (not counting `Delete` operations),
      # stripping away all metadata to return only the value. If you need
      # metadata such as `revision`, `timestamp`, or `operation`, or if you need
      # to be able to get deleted keys, you should use `Bucket#get` instead.
      def []?(key : String) : Bytes?
        if entry = get(key, ignore_deletes: true)
          entry.value
        end
      end

      # Get the value of a key as a `KV::Entry` - raises `KeyError` if the key
      # does not exist or if it's been deleted with `ignore_deletes` set to
      # `true`.
      def get!(key : String, ignore_deletes = false) : Entry
        if value = get(key, ignore_deletes: ignore_deletes)
          value
        else
          raise KeyError.new("Key #{key.inspect} expected, but not found")
        end
      end

      # Creates the given `key` with the given `value` if and only if the key
      # does not yet exist.
      def create(key : String, value : String)
        @kv.create name, key, value
      end

      # Updates the given `key` with the given `value` if and only if it exists
      # and is currently at the given `revision`. If you do not have the latest
      # revision, this method returns `nil` so you can perform domain-specific
      # conflict resolution. If you need to set the key regardless of revision,
      # use `Bucket#put` instead.
      def update(key : String, value : String, revision : Int64)
        @kv.update name, key, value, revision
      end

      # Deletes the given `key` from the KV store. Inside the NATS server, this
      # is implemented as adding another message to the stream that signifies
      # deletion.
      def delete(key : String)
        @kv.delete name, key
      end

      # Purges the given `key` from the KV store. Inside the NATS server, this
      # is implemented as rolling up all versions of this key into a single
      # message with its `KV::Entry#operation` value (`KV-Operation` header)
      # set to `KV::Entry::Operation::Purge`.
      def purge(key : String)
        @kv.purge name, key
      end

      # List all known keys for this bucket, returned as a `Set(String)`.
      def keys
        @kv.keys(name)
      end

      # Get the history
      def history(key : String)
        @kv.history(name, key)
      end

      # Watch the given key (or wildcard) for changes and yielding them to the
      # block. By default, this will also yield deleted messages. To avoid that,
      # pass `ignore_deletes: true`.
      #
      # ```
      # bucket.watch("session.*") do |entry|
      #   _prefix, session_id = entry.subject.split('.', 2)
      #   if entry.operation.deleted?
      #     # do deleted things
      #   else
      #     # the session was updated
      #   end
      # end
      # ```
      #
      # This method blocks until the yielded `Watch` is stopped (use
      # `watch.stop`), so if you want to run it in the background, you will need
      # to run this method inside a `spawn` block.
      #
      # You can also use this method to wait for a specific key to change once:
      #
      # ```
      # bucket.watch my_key do |entry, watch|
      #   # react to the key change
      # ensure
      #   watch.stop # exit the block
      # end
      # ```
      def watch(key : String, *, ignore_deletes = false, &block : Entry, Watch ->)
        @kv.watch(name, key, ignore_deletes: ignore_deletes, &block)
      end
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
        replicas : Int32 = 1,
        allow_rollup : Bool = true,
        deny_delete : Bool = true
      )
        stream = @nats.jetstream.stream.create(
          name: "KV_#{bucket}",
          description: description,
          subjects: ["$KV.#{bucket}.>"],
          max_msgs_per_subject: history.try(&.to_i64),
          max_bytes: max_bytes,
          max_age: ttl,
          max_msg_size: max_value_size,
          storage: storage,
          replicas: {replicas, 1}.max,
          allow_rollup_headers: allow_rollup,
          deny_delete: deny_delete,
        )

        Bucket.new(stream, self)
      end

      def get_bucket(name : String) : Bucket?
        if stream = @nats.jetstream.stream.info("KV_#{name}")
          Bucket.new(stream, self)
        end
      end

      # Assign `value` to `key` in `bucket`.
      def put(bucket : String, key : String, value : String | Bytes) : Int64
        validate_key! key
        case response = @nats.jetstream.publish("$KV.#{bucket}.#{key}", value)
        in JetStream::API::V1::PubAck
          response.sequence
        in JetStream::API::V1::ErrorResponse
          raise Error.new(response.error.description)
        in Nil
          raise Error.new("No response received from the NATS server when setting #{key.inspect} on KV #{bucket.inspect}")
        end
      end

      def set(bucket : String, key : String, value : Data)
        @nats.publish("$KV.#{bucket}.#{key}", value)
      end

      # Get the value associated with the current
      def get(bucket : String, key : String, ignore_deletes = false) : Entry?
        validate_key! key unless key == ">"

        if response = @nats.jetstream.stream.get_msg("KV_#{bucket}", last_by_subject: "$KV.#{bucket}.#{key}")
          operation = Entry::Operation::Put

          case response.message.headers.try { |h| h["KV-Operation"]? }
          when "DEL"
            operation = Entry::Operation::Delete
          when "PURGE"
            operation = Entry::Operation::Purge
          end
          return nil if ignore_deletes && !operation.put?

          _, bucket_name, key_name = response.message.subject.split('.', 3)
          get = Entry.new(
            bucket: bucket_name,
            key: key_name,
            value: response.message.data,
            revision: response.message.seq,
            created_at: response.message.time,
            operation: operation,
          )
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
        revision = update bucket, key, value, revision: 0

        if revision
          revision
        elsif (entry = get(bucket, key)) && !entry.operation.put?
          update(bucket, key, value, revision: entry.revision)
        end
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
        case response = @nats.jetstream.publish "$KV.#{bucket}.#{key}", value, headers: headers
        in JetStream::API::V1::PubAck
          response.sequence
        in JetStream::API::V1::ErrorResponse
          # https://github.com/nats-io/nats-server/blob/3f12216fcc349ae0f7af779c6a4647209fbbe9ab/server/errors.json#L62-L71
          if response.error.err_code.stream_wrong_last_sequence?
            nil
          else
            raise Error.new(response.error.description)
          end
        in Nil
          raise Error.new("No response received from the NATS server when updating #{key.inspect} on KV #{bucket.inspect}")
        end
      end

      # Get all of the keys for the given bucket name
      def keys(bucket : String) : Set(String)
        keys = Set(String).new

        if stream = @nats.jetstream.stream.info("KV_#{bucket}")
          # If there are no messages in the stream just return the empty set of
          # keys. Otherwise, we will end up sitting here waiting for keys to
          # come streaming in.
          if stream.state.messages == 0
            return keys
          end
        end

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

      def history(bucket : String, key : String) : Array(Entry)
        history = [] of Entry

        if stream = @nats.jetstream.stream.info("KV_#{bucket}")
          # If there are no messages in the stream just return the empty list of
          # entries. Otherwise, we will end up sitting here waiting for entries
          # to come streaming in.
          if stream.state.messages == 0
            return history
          end
        end

        watch bucket, key, include_history: true do |msg, watch|
          history << msg
          watch.stop if msg.delta == 0
        end

        history
      end

      def watch(
        bucket : String,
        key : String,
        *,
        ignore_deletes = false,
        include_history = false,
        &block : Entry, Watch ->
      )
        stop_channel = Channel(Nil).new
        watch = Watch.new(stop_channel)
        inbox = "$WATCH_INBOX.#{Random::Secure.hex}"
        deliver_group = Random::Secure.hex
        if include_history
          deliver_policy = JetStream::API::V1::ConsumerConfig::DeliverPolicy::All
        else
          deliver_policy = JetStream::API::V1::ConsumerConfig::DeliverPolicy::LastPerSubject
        end

        stream_name = "KV_#{bucket}"
        consumer = @nats.jetstream.consumer.create(
          stream_name: stream_name,
          deliver_subject: inbox,
          deliver_group: deliver_group,
          deliver_policy: deliver_policy,
          filter_subject: "$KV.#{bucket}.#{key}",
        )
        subscription = @nats.subscribe inbox, queue_group: deliver_group do |msg|
          js_msg = JetStream::Message.new(msg)
          @nats.jetstream.ack js_msg

          operation = Entry::Operation::Put
          case msg.headers.try { |h| h["KV-Operation"]? }
          when "DEL"
            operation = Entry::Operation::Delete
          when "PURGE"
            operation = Entry::Operation::Purge
          end

          if !ignore_deletes || operation.put?
            _, bucket_name, key_name = msg.subject.split('.', 3)
            get = Entry.new(
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

      def delete(bucket : String, key : String)
        validate_key! key

        headers = Headers{"KV-Operation" => "DEL"}
        case response = @nats.jetstream.publish "$KV.#{bucket}.#{key}", "", headers: headers
        in JetStream::API::V1::PubAck
          response
        in JetStream::API::V1::ErrorResponse
          raise Error.new(response.error.description)
        in Nil
          raise Error.new("No response received from the NATS server when deleting #{key.inspect} on KV #{bucket.inspect}")
        end
      end

      def purge(bucket : String, key : String)
        headers = Headers{
          "KV-Operation" => "PURGE",
          "Nats-Rollup"  => "sub",
        }
        case response = @nats.jetstream.publish "$KV.#{bucket}.#{key}", "", headers: headers
        in JetStream::API::V1::PubAck
          response
        in JetStream::API::V1::ErrorResponse
          raise Error.new(response.error.description)
        in Nil
          raise Error.new("No response received from the NATS server when purging #{key.inspect} on KV #{bucket.inspect}")
        end
      end

      def delete(bucket : Bucket)
        delete_bucket bucket.name
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

    struct Entry
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

    class Watch
      def initialize(@stop_channel : Channel(Nil))
      end

      def stop
        @stop_channel.send nil
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
