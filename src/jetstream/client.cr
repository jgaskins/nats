require "../nats"
require "./streams"
require "./consumers"
require "./nak_backoff"
require "./pub_ack"
require "./pull_subscription"

module NATS::JetStream
  # This class provides a client for NATS JetStream for at-least-once delivery.
  # You can either instantiate it with a NATS client or with the
  # `NATS::Client#jetstream` method as a shortcut.
  class Client
    def initialize(@nats : ::NATS::Client)
    end

    # Returns an `API::Stream` instance to interact with the NATS JetStream
    # API for streams.
    def stream
      Streams.new(@nats)
    end

    # Returns an `API::Consumer` instance to interact with the NATS JetStream
    # API for consumers.
    def consumer
      Consumers.new(@nats)
    end

    def publish(
      subject : String,
      body : Data,
      timeout : Time::Span = 2.seconds,
      headers : Headers = Headers.new,
      message_id : String? = nil,
      expected_last_message_id : String? = nil,
      expected_last_sequence : Int64? = nil,
      expected_stream : String? = nil,
      expected_last_subject_sequence : Int64? = nil,
    )
      headers["Nats-Msg-Id"] = message_id if message_id
      headers["Nats-Expected-Last-Msg-Id"] = expected_last_message_id if expected_last_message_id
      headers["Nats-Expected-Stream"] = expected_stream if expected_stream
      headers["Nats-Expected-Last-Sequence"] = expected_last_sequence if expected_last_sequence
      headers["Nats-Expected-Last-Subject-Sequence"] = expected_last_subject_sequence.to_s if expected_last_subject_sequence

      if response = @nats.request(subject, body, timeout: timeout, headers: headers)
        (PubAck | ErrorResponse).from_json(String.new(response.body))
      end
    end

    # Subscribe to messages delivered to the given consumer. Note that this
    # consumer _must_ be a push-based consumer. Pull-based consumers do not
    # allow subscriptions because you must explicitly request the next
    # message.
    #
    # ```
    # js = nats.jetstream
    # consumer = js.consumer.info("orders", "fulfillment")
    # js.subscribe consumer do |msg|
    #   # ...
    #
    #   js.ack msg
    # end
    # ```
    def subscribe(consumer : JetStream::Consumer, &block : Message ->)
      if subject = consumer.config.deliver_subject
        subscribe subject, queue_group: consumer.config.deliver_group, &block
      else
        raise ArgumentError.new("Consumer is not a push consumer (no `deliver_subject`)")
      end
    end

    # Subscribe to the given subject with an optional queue group. This is
    # effectively identical to `NATS::Client#subscribe`, but the message
    # yielded to the block is a `NATS::JetStream::Message` instead of
    # a `NATS::Message`.
    #
    # ```
    # js = nats.jetstream
    # js.subscribe "orders.*", queue_group: "fulfillment" do |msg|
    #   # ...
    #
    #   js.ack msg
    # end
    # ```
    #
    # _NOTE:_ If provided, the `queue_group` _must_ be the same as a `Consumer`'s `deliver_group` for NATS server 2.4.0 and above.
    def subscribe(subject : String, queue_group : String? = nil, &block : Message ->)
      @nats.subscribe subject, queue_group: queue_group do |msg|
        unless (headers = msg.headers) && headers["Status"]? == "100 Idle Heartbeat"
          block.call Message.new(msg)
        end
      end
    end

    @[Experimental("NATS JetStream pull subscriptions may be unstable")]
    def pull_each(consumer : Consumer, *, batch_size : Int, &) : Nil
      pull_subscribe consumer, batch_size: batch_size do |msgs, pull|
        msgs.each { |msg| yield msg, pull }
      end
    end

    @[Experimental("NATS JetStream pull subscriptions may be unstable")]
    def pull_subscribe(consumer : Consumer, *, batch_size : Int, &) : Nil
      pull = pull_subscribe(consumer, backlog: batch_size)
      until pull.closed?
        yield pull.fetch(batch_size), pull
      end
    end

    @[Experimental("NATS JetStream pull subscriptions may be unstable")]
    def pull_subscribe(consumer : Consumer, backlog : Int = 64)
      if consumer.config.deliver_subject
        raise ArgumentError.new("Cannot set up a pull-based subscription to a push-based consumer: #{consumer.name.inspect} on stream #{consumer.stream_name.inspect}")
      end

      PullSubscription.new(consumer, @nats)
    end

    # Acknowledge success processing the specified message, usually called at
    # the end of your subscription block.
    #
    # NOTE: This method is asynchronous. If you need a guarantee that NATS has
    # received your acknowledgement, use `ack_sync` instead.
    #
    # ```
    # jetstream.subscribe consumer do |msg|
    #   # ...
    #
    #   jetstream.ack msg
    # end
    # ```
    def ack(msg : Message)
      @nats.publish msg.reply_to, "+ACK"
    end

    # Acknowledge the given message, waiting on the NATS server to acknowledge
    # your acknowledgement so that you can be sure it was delivered to the
    # server and not simply caught up in the output buffer.
    #
    # ```
    # jetstream.subscribe consumer do |msg|
    #   # ...
    #
    #   jetstream.ack_sync msg
    #   # By the time you get here, the NATS server knows you've acknowledged.
    # end
    # ```
    def ack_sync(msg : Message, timeout : Time::Span = 2.seconds)
      @nats.request msg.reply_to, "+ACK", timeout: timeout
    end

    def ack_sync(messages : Enumerable(Message), timeout : Time::Span = 2.seconds)
      channel = Channel(Bool).new(messages.size)
      messages.each do |msg|
        @nats.request msg.reply_to, "+ACK", timeout: timeout do |response|
          channel.send true
        end
      end

      count = 0
      messages.each do
        select
        when channel.receive
          count += 1
        when timeout(timeout)
        end
      end
      count
    end

    # Notify the NATS server that you need more time to process this message,
    # usually used when a consumer requires that you acknowledge a message
    # within a certain amount of time but a given message is taking longer
    # than expected.
    #
    # ```
    # jetstream.subscribe consumer do |msg|
    #   start = Time.monotonic
    #   users.each do |user|
    #     # If we're running out of time, reset the timer.
    #     if Time.monotonic - start > 25.seconds
    #       jetstream.in_progress msg
    #     end
    #
    #     process(msg)
    #   end
    # end
    # ```
    def in_progress(msg : Message)
      @nats.publish msg.reply_to, "+WPI"
    end

    # Negatively acknowledge the processing of a message, typically called
    # when an exception is raised while processing.
    #
    # ```
    # jetstream.subscribe consumer do |msg|
    #   # doing some work
    #
    #   jetstream.ack msg # Successfully processed
    # rescue ex
    #   jetstream.nack msg # Processing was unsuccessful, try again.
    # end
    # ```
    def nack(msg : Message)
      @nats.publish msg.reply_to, "-NAK"
    end

    # Deliver a negative acknowledgement for the given message and tell the
    # NATS server to delay sending it based on the `NAKBackoff` pattern
    # specified.
    #
    # ```
    # jetstream.subscribe consumer do |msg|
    #   # do some work
    #   jetstream.ack msg
    # rescue
    #   # Use exponential backoff of up to an hour for retries
    #   jetstream.nack msg, backoff: :exponential, max: 1.hour
    # end
    # ```
    def nack(msg : Message, *, backoff : NAKBackoff, max : Time::Span = 1.day)
      case backoff
      in .exponential?
        delay = {(2.0**(msg.delivered_count - 5)).seconds, max}.min
      in .linear?
        delay = {msg.delivered_count.seconds, max}.min
      end

      nack msg, delay: delay
    end

    # Deliver a negative acknowledgement for the given message and tell the
    # NATS server to delay sending it for the given time span.
    #
    # ```
    # jetstream.subscribe consumer do |msg|
    #   # do some work
    #   jetstream.ack msg
    # rescue
    #   # Use exponential backoff of up to an hour for retries
    #   jetstream.nack msg, delay: 30.seconds
    # end
    # ```
    def nack(msg : Message, *, delay : Time::Span)
      @nats.publish msg.reply_to, %(-NAK {"delay":#{delay.total_nanoseconds.to_i64}})
    end
  end
end
