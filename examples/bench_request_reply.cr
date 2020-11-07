require "../src/nats"

nats = NATS::Client.new

subscription = nats.subscribe "foo" do |msg|
  if msg.reply_to
    nats.reply msg, msg.body
  end
end

count = 0i64
total = 0i64
start = Time.monotonic
loop do
  nats.request "foo", "bar", timeout: 2.seconds
  total += 1i64
  count += 1i64

  if count >= 1000
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
