require "../src/nats"

nats = NATS::Client.new
# Ensure we close the client explicitly to flush any buffered messages
at_exit { nats.close }

nats.subscribe "foo" do |msg|
  pp msg
end

nats.publish "foo", "bar"
