require "../nats"

module NATS::JetStream
    # A `NATS::JetStream::Message` is very similar to a `NATS::Message` in that
    # it represents a piece of information published by a NATS client (not
    # necessarily _this_ NATS client, though). This `Message` type contains more
    # information, however, such as information about the stream and consumer
    # it came from, how many times it's been delivered, etc.
    struct Message
      # The name of the stream this message was consumed from
      getter stream : String

      # The name of the consumer we received this message from
      getter consumer : String

      # The number of times this particular message has been delivered by this
      # consumer, starting at 1
      getter delivered_count : Int64

      # The position of this message within its stream
      getter stream_seq : Int64

      # The position of this message within its consumer, including redeliveries
      getter consumer_seq : Int64

      # When this message was originally published
      getter timestamp : Time

      # How many messages follow this message for this consumer
      getter pending : Int64

      # The original body of the message, encoded as binary. If you need text,
      # wrap the body in a `String`.
      #
      # ```
      # jetstream.subscribe consumer do |msg|
      #   body_string = String.new(msg.body)
      #
      #   # ...
      # end
      # ```
      getter body : Bytes

      # The original subject this message was published to, which can be (and
      # most likely is) different from the subject it was delivered to
      getter subject : String

      # The subject used for acknowledging this message
      getter reply_to : String

      # Any headers that were published with this message, including ones
      # interpreted by the NATS server, such as `Nats-Msg-Id` for message
      # deduplication.
      getter headers : Headers { Headers.new }

      # Instantiate a `NATS::JetStream::Message` based on a `NATS::Message`.
      # Used by JetStream subscriptions to build `JetStream::Message`
      # instances, since JetStream is a layer on top of core NATS.
      def self.new(msg : ::NATS::Message)
        # reply_to format:
        # $JS.ACK.<stream>.<consumer>.<delivered count>.<stream sequence>.<consumer sequence>.<timestamp>.<pending messages>
        if reply_to = msg.reply_to
          # TODO: figure out if it's worth optimizing to avoid the array allocation
          _jetstream, _ack, stream, consumer, delivered_count, stream_seq, consumer_seq, timestamp, pending_messages = reply_to.split('.')
          new(
            stream: stream,
            consumer: consumer,
            delivered_count: delivered_count.to_i64,
            stream_seq: stream_seq.to_i64,
            consumer_seq: consumer_seq.to_i64,
            timestamp: Time::UNIX_EPOCH + timestamp.to_i64.nanoseconds,
            pending: pending_messages.to_i64,
            body: msg.body,
            subject: msg.subject,
            reply_to: reply_to,
            headers: msg.headers,
          )
        else
          raise InvalidJetStreamMessage.new("JetStream messages must have the reply_to field")
        end
      end

      def initialize(@stream, @consumer, @delivered_count, @stream_seq, @consumer_seq, @timestamp, @pending, @body, @subject, @reply_to, @headers)
      end

      class InvalidJetStreamMessage < Exception
      end
    end
end
