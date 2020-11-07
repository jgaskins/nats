require "../src/nats"

nats = NATS::Client.new

subscription = nats.subscribe "foo" do |msg|
  if msg.reply_to
    nats.reply msg, String.new(msg.body).upcase.to_slice
  end
end

puts "Sending request..."
started_at = Time.monotonic
if response = nats.request("foo", "bar", timeout: 2.seconds)
  puts "Received #{String.new(response.body)} in #{Time.monotonic - started_at}"
end
