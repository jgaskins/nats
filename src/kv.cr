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
  @[Experimental("NATS KV support is experimental and subject to change as NATS support for it changes")]
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
      # The name of this bucket
      getter name : String

      # The name of the underlying `JetStream` stream.
      getter stream_name : String

      # An optional description of the purpose of this bucket
      getter description : String?

      # The maximum number of bytes in a value
      getter max_value_size : Int32?

      # The number of revisions NATS will retain for this key
      getter history : Int64?

      # The maximum length of time NATS will retain a value or revision for a key
      getter ttl : Time::Span?

      # The maximum size in bytes of this bucket in memory or on disk
      getter max_bytes : Int64?

      # Where NATS stores the data for this bucket
      getter storage : JetStream::API::V1::StreamConfig::Storage

      # The number of NATS nodes to which this KV bucket will be replicated
      getter replicas : Int32?

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

      def each_key
        @kv.each_key(name) { |key| yield key }
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
      # watch = bucket.watch(my_key)
      # watch.each do |entry|
      #   # react to the key change
      #
      #   watch.stop if entry.latest? # exit the block
      # end
      # ```
      def watch(key : String, *, ignore_deletes = false, include_history = true, &block : Entry ->)
        @kv.watch(name, key, ignore_deletes: ignore_deletes, include_history: include_history, &block)
      end
    end

    class Client
      def initialize(@nats : ::NATS::Client)
      end

      # Create a `NATS::KV::Bucket` to store key/value mappings in with the
      # specified `name`. Options are:
      #
      # - `storage`: where to store the data for this bucket, either `:file` or `:memory`
      # - `max_value_size`: the largest number of bytes a key/value entry can hold
      # - `history`: how many revisions of a key to retain
      # - `ttl`: how long until a value or revision expires
      # - `max_bytes`: maximum size of this bucket on disk or in memory
      # - `replicas`: how many NATS nodes to store this bucket's data on
      def create_bucket(
        name : String,
        description : String = "",
        *,
        max_value_size : Int32? = nil,
        history : UInt8? = nil,
        ttl : Time::Span? = nil,
        max_bytes : Int64? = nil,
        storage : JetStream::API::V1::StreamConfig::Storage = :file,
        replicas : Int32 = 1
      ) : Bucket
        unless name =~ /\A[a-zA-Z0-9_-]+\z/
          raise ArgumentError.new("NATS KV bucket names can only contain alphanumeric characters, underscores, and dashes")
        end

        # https://github.com/nats-io/nats.go/blob/d7c1d78a50fc9cded3814ae7d7176fa66b73a4b0/kv.go#L295-L307
        stream = @nats.jetstream.stream.create(
          name: "KV_#{name}",
          description: description,
          subjects: ["$KV.#{name}.>"],
          max_msgs_per_subject: history.try(&.to_i64),
          max_bytes: max_bytes,
          max_age: ttl,
          max_msg_size: max_value_size,
          storage: storage,
          replicas: {replicas, 1}.max,
          allow_rollup_headers: true,
          deny_delete: true,
        )

        Bucket.new(stream, self)
      end

      # Get the `Bucket` with the given name, or `nil` if that bucket does not exist.
      def get_bucket(name : String) : Bucket?
        if stream = @nats.jetstream.stream.info("KV_#{name}")
          Bucket.new(stream, self)
        end
      end

      # Assign `value` to `key` in `bucket`, returning an acknowledgement or error if the key could not be set.
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

      # Assign `value` to `key` in `bucket` without waiting for acknowledgement
      # from the NATS server.
      def set(bucket : String, key : String, value : Data)
        @nats.publish("$KV.#{bucket}.#{key}", value)
      end

      # Get the `KV::Entry` for the given `key` in `bucket`, or `nil` if the key
      # does not exist.
      def get(bucket : String, key : String, ignore_deletes = false) : Entry?
        if response = @nats.jetstream.stream.get_msg("KV_#{bucket}", last_by_subject: "$KV.#{bucket}.#{key}")
          operation = Entry::Operation::Put

          case response.message.headers.try(&.["KV-Operation"]?)
          when "DEL"
            operation = Entry::Operation::Delete
          when "PURGE"
            operation = Entry::Operation::Purge
          end
          return nil if ignore_deletes && !operation.put?

          _, bucket_name, key_name = response.message.subject.split('.', 3)
          Entry.new(
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

      # Get all of the keys matching `pattern` for the given `bucket` name.
      def keys(bucket : String, pattern : String = ">") : Set(String)
        keys = Set(String).new

        # If there are no messages in the stream with this pattern, just return
        # the empty set of keys. Otherwise, we will end up sitting here waiting
        # for keys to come streaming in.
        return keys if get(bucket, pattern).nil?

        # Look at all the keys in the current bucket
        watch = watch(bucket, pattern)
        watch.each do |entry|
          case entry.operation
          when .delete?, .purge?
            keys.delete entry.key
          else
            keys << entry.key
          end
          watch.stop if entry.delta == 0
        end

        keys
      end

      def each_key(bucket : String, pattern : String = ">") : Nil
        watch = watch(bucket, pattern, include_history: false)
        watch.each do |entry|
          yield entry.key if entry.operation.put?
          watch.stop if entry.latest?
        end
      end

      # Get all of the currently retained history for the given `key` in the given `bucket` name. Note that some of the history could have expired due to `Bucket#ttl` or `Bucket#history`.
      #
      # ```
      # kv.set "config", "name", "1"
      # kv.history("config", "name")
      # ```
      def history(bucket : String, key : String) : Array(Entry)
        history = [] of Entry

        return history if get(bucket, key).nil?

        watch = watch(bucket, key, include_history: true)
        watch.each do |msg|
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
        include_history = false
      )
        inbox = "$KV_WATCH_INBOX.#{NUID.next}"
        deliver_group = NUID.next
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
          ack_policy: :none,
        )
        Watch.new(
          bucket: bucket,
          key: key,
          nats: @nats,
          consumer: consumer,
          ignore_deletes: ignore_deletes,
        )
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

      def value_string
        String.new value
      end

      def latest?
        delta == 0
      end
    end

    class Watch
      include Iterable(Entry)
      include Enumerable(Entry)

      private OPERATIONS = {
        "DEL"   => Entry::Operation::Delete,
        "PURGE" => Entry::Operation::Purge,
      }

      @subscription : NATS::Subscription

      def initialize(
        @bucket : String,
        @key : String,
        @nats : ::NATS::Client,
        @consumer : JetStream::API::V1::Consumer,
        ignore_deletes : Bool
      )
        @channel = Channel(Entry).new
        @subscription = @nats.subscribe consumer.config.deliver_subject.not_nil!, queue_group: consumer.config.deliver_group do |msg|
          js_msg = JetStream::Message.new(msg)

          operation = OPERATIONS.fetch(
            msg.headers.try(&.["KV-Operation"]?),
            Entry::Operation::Put,
          )

          if !ignore_deletes || operation.put?
            _, bucket_name, key_name = msg.subject.split('.', 3)
            entry = Entry.new(
              bucket: bucket_name,
              key: key_name,
              value: msg.body,
              revision: js_msg.stream_seq,
              created_at: js_msg.timestamp,
              delta: js_msg.pending,
              operation: operation,
            )

            @channel.send entry
          end
        end
      end

      def each
        Iterator.new(@channel)
      end

      def each
        each.each { |entry| yield entry }
      end

      def stop
        @channel.close
        @nats.unsubscribe @subscription
        @nats.jetstream.consumer.delete @consumer.stream_name, @consumer.name
      end

      struct Iterator
        include ::Iterator(Entry)

        def initialize(@channel : Channel(Entry))
        end

        def next
          @channel.receive? || stop
        end
      end
    end
  end

  class Client
    # Returns a `NATS::KV::Client` that uses this client's connection to
    # the NATS server.
    @[Experimental("NATS KV support is experimental and subject to change as NATS support for it changes")]
    def kv
      @kv ||= KV::Client.new(self)
    end
  end
end
