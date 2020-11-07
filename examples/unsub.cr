require "../src/nats"

nats = NATS::Client.new

subscription = nats.subscribe "foo" do |msg|
  puts String.new msg.body
end

nats.unsubscribe subscription.sid, max_messages: 3

500.times do |i|
  nats.publish "foo", i.to_s
end

sleep 1.second
