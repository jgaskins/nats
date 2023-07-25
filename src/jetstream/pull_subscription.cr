require "../nats"
require "./consumer"
require "./message"

module NATS::JetStream
  class PullSubscription
    getter consumer : Consumer
    @nats : NATS::Client

    def initialize(@consumer, @nats)
    end

    def fetch(timeout : Time::Span = 2.seconds)
      fetch(1, timeout: timeout).first?
    end

    def fetch(message_count : Int, timeout : Time::Span = 2.seconds, no_wait : Bool = false, max_bytes : Int? = nil) : Enumerable(Message)
      # We have to reimplement request/reply here because we get N replies
      # for 1 request, which NATS request/reply does not support.
      LOG.trace { "Creating channel" }
      channel = Channel(Message).new(message_count)
      deliver_subject = "_INBOX.#{NUID.next}"
      LOG.trace { "Sending subscription" }
      subscription = @nats.subscribe deliver_subject do |msg, sub|
        # If there isn't a reply-to, we're probably getting an update about
        # some logistics pertaining to this pull subscription.
        if (headers = msg.headers) && (status = headers["Status"]?) && status.starts_with?("409")
          sub.close
        elsif msg.reply_to.presence
          # We can only receive messages that can be replied to because that's
          # how we ack them.
          channel.send Message.new(msg)
        else
          raise InternalError.new("Found a bug in NATS pull subscriptions. Please check the issues at https://github.com/jgaskins/nats/issues and, if there is not one open yet, please open one with this stack trace.")
        end
      end

      @nats.publish "$JS.API.CONSUMER.MSG.NEXT.#{consumer.stream_name}.#{consumer.name}",
        message: {
          expires: {(timeout - 100.milliseconds).total_nanoseconds, 0}
            .max
            .to_i64,
          batch: message_count,
        }.to_json,
        reply_to: deliver_subject
      @nats.flush!

      msgs = Array(Message).new(initial_capacity: message_count)
      total_timeout = timeout
      finish_by = Time.monotonic + timeout

      message_count.times do |i|
        LOG.trace { "Waiting for message #{i}" }
        select
        when msg = channel.receive
          LOG.trace { "received" }
          msgs << msg
        when timeout(finish_by - Time.monotonic)
          LOG.trace { "timed out" }
          break
        end
      end
      @nats.unsubscribe subscription

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
    def ack_next(msg : Message, timeout : Time::Span = 2.seconds)
      # ack_next(msg, 1, timeout, no_wait).first?
      inbox = "ack-next.#{NUID.next}"
      channel = Channel(Message).new(1)

      begin
        sub = @nats.subscribe inbox do |msg|
          channel.send Message.new(msg)
        end
        @nats.unsubscribe sub, max_messages: 1

        @nats.publish(msg.reply_to, "+NXT 1", reply_to: inbox)

        select
        when reply = channel.receive
          reply
        when timeout(timeout)
          @nats.unsubscribe sub
          nil
        end
      end
    end

    # Acknowledge the given message and request the next `count` messages in
    # a single round trip to the server to save latency.
    def ack_next(msg : Message, count : Int, timeout : Time::Span = 2.seconds, no_wait : Bool = false)
      # This is an async ack, and then we fetch synchronously
      nats.jetstream.ack msg
      fetch(count, timeout: timeout)
    end

    class InternalError < Error
    end
  end
end
