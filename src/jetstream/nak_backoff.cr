module NATS::JetStream
  # Common `nack` backoff strategies give a simple shorthand for setting the
  # delays on subsequent delivery attempts.
  #
  # ```
  # nats = NATS::Client.new
  #
  # jetstream.subscribe consumer do |msg|
  #   # do some work
  #   jetstream.ack msg
  # rescue ex
  #   jetstream.nack msg, backoff: :exponential
  #   raise ex
  # end
  # ```
  enum NAKBackoff
    # Exponential backoff starts at a delay of 1/16th of a second and
    # doubles every time the message is nacked with this strategy. This has
    # the effect of running the first 3 attempts roughly immediately so that
    # an uncommon but not unexpected failure (API call in a downstream
    # service returns a 500) doesn't need to wait several seconds unless it
    # truly needs to.
    Exponential

    # Linear backoff delays for 1 second for each time the message is nacked
    # with this strategy.
    Linear
  end
end
