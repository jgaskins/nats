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
200.times do
  spawn do
    loop do
      nats.request "foo", "bar", timeout: 2.seconds
      total += 1i64
      count += 1i64

      if count >= 100_000
        pp outgoing_msg_per_sec: (count // (Time.monotonic - start).total_seconds).format
        count = 0i64
        start = Time.monotonic
        Fiber.yield
      end
    rescue ex
      pp ex
      pp total: total
      break
    end
  end
end

sleep
