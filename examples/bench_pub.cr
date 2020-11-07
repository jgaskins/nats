require "../src/nats"

nats = Array.new(3) { NATS::Client.new }

# nats.subscribe "foo" do |msg|
#   pp msg
# end

# nats.publish "foo", "bar"

# sleep 1.second
# exit 0

count = 0i64
total = 0i64
start = Time.monotonic
loop do
  nats.sample.publish "foo", "bar"
  total += 1i64
  count += 1i64

  if count >= 10_000_000i64
    pp outgoing_msg_per_sec: (count // (Time.monotonic - start).total_seconds).format
    Fiber.yield
    count = 0i64
    start = Time.monotonic
  end
rescue ex
  pp ex
  pp total: total
  break
end
