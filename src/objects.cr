require "digest"

require "./nats"
require "./jetstream"
require "./nuid"

module NATS
  # The NATS object store is S3-style object storage backed by NATS JetStream.
  #
  # You can access the object store API with `NATS::Client#objects`.
  #
  # Create a bucket with `Objects::Client#create_bucket`:
  #
  # ```
  # bucket = nats.objects.create_bucket("my-bucket")
  # ```
  #
  # Add an object from a file on disk (for example, from a tempfile created from an HTTP file upload) with `Objects::Bucket#put`:
  #
  # ```
  # File.open filename do |file|
  #   bucket.put "my-key", file
  # end
  # ```
  #
  # Get the metadata from an object in the store with `Objects::Bucket#get_info`:
  #
  # ```
  # bucket.get_info("my-key")
  # ```
  #
  # Get the contents of an object in the store with `Objects::Bucket#get`, which returns an `IO` instance that you can read from gradually to avoid having to load the entire object into memory. For example, for storing large images or videos.
  #
  # ```
  # if io = bucket.get("my-key")
  #   io.gets_to_end
  # end
  # ```
  @[Experimental("NATS object store is experimental and the API could change")]
  module Objects
    class Error < ::NATS::Error
    end

    class KeyError < Error
    end

    DEFAULT_CHUNK_SIZE = 128 * 1024 # 128KB

    class Client
      def initialize(@nats : NATS::Client)
      end

      # Create a bucket in the object store
      #
      # ```
      # bucket = nats.objects.create_bucket("my-bucket")
      # ```
      #
      # Give the bucket a description that will show up when you get bucket metadata
      # ```
      # bucket = nats.objects.create_bucket("uploads", description: "Storage for user uploads")
      # ```
      #
      # Set a maximum lifetime for your objects, after which they are deleted:
      # ```
      # bucket = nats.objects.create_bucket("snapchat-images", ttl: 1.day)
      # ```
      #
      # Replicate your objects across multiple NATS servers in your cluster:
      # ```
      # bucket = nats.objects.create_bucket("durable-storage", replicas: 3)
      # ```
      def create_bucket(
        name : String,
        description : String = "",
        *,
        ttl : Time::Span? = nil,
        storage : JetStream::API::V1::StreamConfig::Storage = :file,
        replicas : Int? = nil,
        max_bytes : Int? = nil,
        placement : JetStream::API::V1::StreamConfig::Placement? = nil
      )
        stream = @nats.jetstream.stream.create(
          name: "OBJ_#{name}",
          description: description,
          subjects: [
            "$O.#{name}.C.>", # Object chunks
            "$O.#{name}.M.>", # Metadata
          ],
          max_age: ttl,
          max_bytes: max_bytes,
          storage: storage,
          discard: :new,
          allow_rollup_headers: true,
          placement: placement,
        )

        Bucket.new(stream, self)
      end

      def get_bucket(name : String)
        if stream = @nats.jetstream.stream.info("OBJ_#{name}")
          Bucket.new(stream, self)
        end
      end

      def delete_bucket(name : String)
        @nats.jetstream.stream.delete "OBJ_#{name}"
      end

      def put(bucket : String, key : String, value : IO, description : String? = nil, headers : Headers = Headers.new, chunk_size : Int = DEFAULT_CHUNK_SIZE)
        existing = get_info(bucket, key)
        id = NUID.next
        stream_name = "OBJ_#{bucket}"
        chunk_subject = "$O.#{bucket}.C.#{id}"
        meta_subject = "$O.#{bucket}.M.#{sanitize_key(key)}"
        chunk = Bytes.new(chunk_size)
        sha = Digest::SHA256.new
        sent = 0
        total = 0i64

        begin
          while (count = value.read(chunk)) != 0
            body = chunk[0...count]
            sha << body

            @nats.publish chunk_subject, body
            sent += 1
            total += count
          end

          msg = ObjectInfo.new(
            bucket: bucket,
            name: key,
            description: description,
            headers: headers,
            nuid: id,
            size: total,
            mtime: Time.utc,
            chunks: sent,
            digest: "SHA-256=#{Base64.urlsafe_encode(sha.final)}",
          )
          @nats.jetstream.publish meta_subject, msg.to_json, headers: Headers{"Nats-Rollup" => "sub"}
        rescue ex
          @nats.jetstream.stream.purge stream_name, subject: chunk_subject
          raise ex
        end

        if existing
          @nats.jetstream.stream.purge stream_name, subject: "$O.#{bucket}.C.#{existing.nuid}"
        end

        @nats.flush
        msg
      end

      def get_info(bucket : String, key : String)
        meta = "$O.#{bucket}.M.#{sanitize_key(key)}"
        stream = "OBJ_#{bucket}"
        if response = @nats.jetstream.stream.get_msg(stream, last_by_subject: meta)
          info = ObjectInfo.from_json(String.new(response.message.data))
          info.mtime = response.message.time
          info
        end
      end

      def get(bucket : String, key : String) : IO?
        return unless info = get_info(bucket, key)

        subject = "NATS.Objects.#{bucket}.data.#{key}.get.#{NUID.next}"
        consumer = @nats.jetstream.consumer.create(
          stream_name: "OBJ_#{bucket}",
          filter_subject: "$O.#{bucket}.C.#{info.nuid}",
          deliver_subject: subject,
          ack_policy: :none,
          max_deliver: 1,
          flow_control: true,
          idle_heartbeat: 5.seconds, # Required for flow control
        )
        read, write = IO.pipe
        chunks = 0
        @nats.subscribe(subject) do |msg, subscription|
          if msg.body.empty? && (headers = msg.headers) && headers["Status"]? == "100 FlowControl Request"
            # Once we ack the flow-control message, we will receive more data.
            # Because we're using IO.pipe, we don't have to implement flow
            # control ourselves here. We get it for free because these pipes
            # have a buffer size that will automatically block the write call
            # below if that buffer fills up.
            @nats.reply msg, "" if msg.reply_to
          else
            # TODO: ensure we get *all* chunks
            write.write msg.body
            chunks += 1
          end

          if chunks >= info.chunks
            write.close
            @nats.jetstream.consumer.delete consumer
            @nats.unsubscribe subscription
          end
        rescue ex : IO::Error
          write.close
          @nats.jetstream.consumer.delete consumer
          @nats.unsubscribe subscription
        end
        read
      end

      def keys(bucket : String, pattern : String = ">") : Set(String)
        keys = Set(String).new

        # If there are no messages in the stream with this pattern, just return
        # the empty set of keys. Otherwise, we will end up sitting here waiting
        # for keys to come streaming in.
        return keys if get_info(bucket, pattern).nil?

        # Look at all the keys in the current bucket
        watch bucket, pattern do |msg, watch|
          keys << msg.name

          watch.stop if watch.pending == 0
        end

        keys
      end

      def watch(
        bucket : String,
        key : String,
        &block : ObjectInfo, Watch ->
      )
        stop_channel = Channel(Nil).new
        watch = Watch.new(stop_channel)
        inbox = "$WATCH_INBOX.#{NUID.next}"
        deliver_group = NUID.next

        stream_name = "OBJ_#{bucket}"
        consumer = @nats.jetstream.consumer.create(
          stream_name: stream_name,
          deliver_subject: inbox,
          deliver_group: deliver_group,
          deliver_policy: :last_per_subject,
          filter_subject: "$O.#{bucket}.M.#{sanitize_key(key)}",
          ack_policy: :none,
        )
        subscription = @nats.subscribe inbox, queue_group: deliver_group do |msg|
          js_msg = JetStream::Message.new(msg)
          watch.pending = js_msg.pending

          _, bucket_name, _, key_name = msg.subject.split('.', 4)
          info = ObjectInfo.from_json String.new msg.body

          block.call info, watch
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

      struct Watch
        property pending : Int64 = 0

        def initialize(@stop_channel : Channel(Nil))
        end

        def stop
          @stop_channel.send nil
        end
      end

      struct ObjectInfo
        include JSON::Serializable

        getter bucket : String
        getter name : String
        getter description : String?
        getter headers : Headers { Headers.new }
        getter nuid : String
        getter size : Int64
        property mtime : Time
        getter chunks : Int32
        getter digest : String
        getter deleted : Bool?

        def initialize(*, @bucket, @name, @description, @headers, @nuid, @size, @chunks, @digest, @mtime = Time.new(0, 0), @deleted = nil)
        end
      end

      private def sanitize_key(key : String)
        key.tr(" .", "__")
      end
    end

    struct Bucket
      getter name : String
      getter stream_name : String
      @client : Objects::Client

      def self.new(stream : JetStream::API::V1::Stream, client : Objects::Client)
        new(
          name: stream.config.name.lchop("OBJ_"),
          stream_name: stream.config.name,
          client: client,
        )
      end

      def initialize(@name, @stream_name, @client)
      end

      def put(key : String, value : String | Bytes, **kwargs)
        put(key, IO::Memory.new(value), **kwargs)
      end

      def put(key : String, value : IO, *, description : String? = nil, headers : Headers = Headers.new, chunk_size : Int = DEFAULT_CHUNK_SIZE)
        @client.put(name, key, value, description: description, headers: headers, chunk_size: chunk_size)
      end

      def get_info!(key : String)
        if info = get_info(key)
          info
        else
          raise KeyError.new("Key #{key.inspect} does not exist for object bucket #{name.inspect}")
        end
      end

      def get_info(key : String)
        @client.get_info name, key
      end

      def get!(key : String)
        if result = get(key)
          result
        else
          raise KeyError.new("Key #{key.inspect} does not exist for object bucket #{name.inspect}")
        end
      end

      def get(key : String)
        @client.get name, key
      end

      def keys(pattern : String = ">")
        @client.keys name, pattern
      end
    end
  end

  class Client
    getter objects : Objects::Client { Objects::Client.new(self) }
  end
end
