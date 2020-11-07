require "../src/nats"

nats = NATS::Client.new

count = 0
start = Time.monotonic
nats.subscribe "foo", queue_group: "bar" do |msg|
  count += 1

  if count >= 500_000
    pp incoming_msgs_per_sec: (count // (Time.monotonic - start).total_seconds).format
    Fiber.yield
    count = 0
    start = Time.monotonic
  end
end

sleep
