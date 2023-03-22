require "../nats"
require "./consumer"
require "./message"

module NATS::JetStream
  class PullSubscription
    getter nats_subscription : ::NATS::Subscription
    getter consumer : Consumer
    getter? closed : Bool = false
    @channel : Channel(Message)
    @nats : NATS::Client

    def initialize(@nats_subscription, @consumer, @channel, @nats)
    end

    def fetch(timeout : Time::Span = 2.seconds)
      fetch(1, timeout: timeout).first?
    end

    def fetch(message_count : Int, timeout : Time::Span = 2.seconds, no_wait : Bool = false, max_bytes : Int? = nil) : Enumerable(Message)
      # We have to reimplement request/reply here because we get N replies
      # for 1 request, which NATS request/reply does not support.
      @nats.publish "$JS.API.CONSUMER.MSG.NEXT.#{consumer.stream_name}.#{consumer.name}",
        message: {
          expires: timeout.total_nanoseconds.to_i64,
          batch:   message_count,
        }.to_json,
        reply_to: @nats_subscription.subject

      msgs = Array(Message).new(initial_capacity: message_count)
      total_timeout = timeout
      start = Time.monotonic
      message_count.times do |i|
        select
        when msg = @channel.receive
          msgs << msg
        when timeout(i == 0 ? total_timeout : 100.microseconds)
          break
        end
      end

      msgs
    end

    # Acknowledge the given message and request the next in a single round
    # trip to the server to save latency.
    #
    # ```
    # pull = js.pull_subscribe(consumer)
    # # Poll for messages until we get one
    # until msg = pull.fetch(timeout: 30.seconds)
    # end
    # loop do
    # end
    # ```
    def ack_next(msg : Message, timeout : Time::Span = 2.seconds, no_wait : Bool = false)
      ack_next(msg, 1, timeout, no_wait).first?
    end

    # Acknowledge the given message and request the next `count` messages in
    # a single round trip to the server to save latency.
    def ack_next(msg : Message, count : Int, timeout : Time::Span = 2.seconds, no_wait : Bool = false)
      body = String.build do |str|
        str << "+NXT "
        {
          batch:   count,
          no_wait: no_wait,
        }.to_json str
      end

      @nats.publish(msg.reply_to, body, reply_to: @nats_subscription.subject)
      msgs = Array(Message).new(initial_capacity: count)
      start = Time.monotonic
      count.times do |i|
        select
        when msg = @channel.receive
          msgs << msg
        when timeout(timeout - (Time.monotonic - start))
          break
        end
      end
      msgs
    end

    def finalize
      close
    end

    def close
      @nats.unsubscribe @nats_subscription
      @closed = true
    end
  end
end
