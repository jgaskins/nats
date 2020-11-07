require "../src/nats"

nats = NATS::Client.new

nats.subscribe "foo" do |msg|
  pp msg
end

nats.publish "foo", "bar"

sleep 1.second
exit 0

count = 0
start = Time.monotonic
loop do
  nats.publish "foo", "bar"
  count += 1

  if count >= 50_000
    pp rate: (count // (Time.monotonic - start).total_seconds).format
    Fiber.yield
    count = 0
    start = Time.monotonic
  end
end
