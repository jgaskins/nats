require "../nats"
require "./consumer"
require "./message"

module NATS::JetStream
  class PullSubscription
    getter consumer : Consumer
    @nats : NATS::Client
    getter? closed = false

    def initialize(@consumer, @nats)
    end

    def fetch(timeout : Time::Span = 2.seconds)
      fetch(1, timeout: timeout).first?
    end

    # Fetch up to `message_count` messages from the consumer, waiting up to
    # `timeout` for them. Returns early once we have `message_count` of them,
    # when the server tells us the request has expired, or, if `no_wait` is
    # `true`, as soon as the server tells us there aren't any more.
    #
    # Raises `NATS::JetStream::Error` if the server rejects the request, such
    # as when the consumer no longer exists.
    def fetch(message_count : Int, timeout : Time::Span = 2.seconds, no_wait : Bool = false, max_bytes : Int? = nil) : Enumerable(Message)
      # We have to reimplement request/reply here because we get N replies
      # for 1 request, which NATS request/reply does not support.
      LOG.trace { "Creating channel" }
      # Room for every message plus the status that ends the request
      channel = Channel(::NATS::Message).new(message_count + 1)
      deliver_subject = "_INBOX.#{NUID.next}"
      LOG.trace { "Sending subscription" }
      subscription = @nats.subscribe deliver_subject do |msg|
        # We handle replies in the fetching fiber so we can raise errors
        # there. Anything we don't have room for arrived after the request
        # was finished.
        select
        when channel.send msg
        else
        end
      end

      request = JSON.build do |json|
        json.object do
          json.field "batch", message_count
          # With `expires`, the server treats `no_wait` as "wait until it
          # expires unless there are messages", so we only send one of them.
          if no_wait
            json.field "no_wait", true
          else
            json.field "expires", {(timeout - 100.milliseconds).total_nanoseconds, 0}.max.to_i64
          end
          json.field "max_bytes", max_bytes if max_bytes
        end
      end
      @nats.publish "$JS.API.CONSUMER.MSG.NEXT.#{consumer.stream_name}.#{consumer.name}",
        data: request,
        reply_to: deliver_subject
      @nats.flush!

      msgs = Array(Message).new(initial_capacity: message_count)
      start = Time.instant

      while msgs.size < message_count
        LOG.trace { "Waiting for message #{msgs.size}" }
        select
        when msg = channel.receive
          # We can only receive messages that can be replied to because that's
          # how we ack them. Anything else is the server telling us about this
          # pull request.
          if msg.reply_to.presence
            LOG.trace { "received" }
            msgs << Message.new(msg)
          elsif status = msg.headers.try(&.["Status"]?)
            description = msg.headers.try(&.["Description"]?)
            LOG.trace { "received status #{status} #{description}" }
            case status
            when .starts_with?("100")
              # Idle heartbeat
            when .starts_with?("404"), .starts_with?("408")
              # No more messages, or the request expired
              break
            when .starts_with?("409")
              # The next message is bigger than the remaining `max_bytes`
              break if description.try(&.includes?("MaxBytes"))
              raise Error.new("Could not fetch from consumer #{consumer.name.inspect} on stream #{consumer.stream_name.inspect}: #{status} #{description}")
            else
              raise Error.new("Could not fetch from consumer #{consumer.name.inspect} on stream #{consumer.stream_name.inspect}: #{status} #{description}")
            end
          else
            raise InternalError.new("Found a bug in NATS pull subscriptions. Please check the issues at https://github.com/jgaskins/nats/issues and, if there is not one open yet, please open one with this stack trace.")
          end
        when timeout({timeout - start.elapsed, Time::Span.zero}.max)
          LOG.trace { "timed out" }
          break
        end
      end

      msgs
    ensure
      @nats.unsubscribe subscription if subscription
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

    def close
      @closed = true
    end

    class InternalError < Error
    end
  end
end
