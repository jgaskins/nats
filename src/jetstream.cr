require "json"
require "./nats"
require "./error"
require "./jetstream/client"

module NATS
  # NATS JetStream provides at-least-once delivery guarantees with the
  # possibility of exactly-once for some use cases, allowing NATS to be used for
  # scenarios where 100% delivery of messages and events is required.
  module JetStream
  end

  class Client
    # Returns a `NATS::JetStream::Client` that uses this client's connection to
    # the NATS server.
    def jetstream
      @jetstream ||= JetStream::Client.new(self)
    end
  end
end
