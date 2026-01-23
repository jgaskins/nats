require "./spec_helper"
require "../src/nats"
require "uuid"
require "wait_group"

nats = NATS::Client.new

describe NATS do
  it "can publish and subscribe to messages" do
    subject = "temp.#{UUID.random}"
    string = ""

    nats.subscribe subject do |msg|
      string = String.new(msg.body)
    end

    nats.publish subject, "foo"
    nats.flush

    string.should eq "foo"
  end

  Hash(Char, String){
    '\0' => "foo\0",
    ' '  => "foo bar",
    '*'  => "foo*",
    '>'  => "<foo>",
  }.each do |invalid_char, subject|
    it "raises ArgumentError if the subject contains #{invalid_char.inspect}" do
      expect_raises ArgumentError do
        nats.publish subject, ""
      end
    end
  end

  it "can set message headers without a reply-to" do
    subject = "temp.#{UUID.random}"
    headers = nil
    body = Bytes.empty

    nats.subscribe subject do |msg|
      headers = msg.headers
      body = msg.body
    end

    nats.publish subject, "asdf", headers: NATS::Message::Headers{"foo" => "bar"}
    nats.flush

    String.new(body).should eq "asdf"
    headers.should eq NATS::Message::Headers{"foo" => "bar"}
  end

  it "can set multiple of the same message header" do
    subject = "temp.#{UUID.random}"
    nats.subscribe subject do |msg|
      nats.reply msg, "", headers: NATS::Message::Headers{
        "one"  => "one",
        "many" => %w[one two],
      }
    end

    unless response = nats.request subject, ""
      raise "Did not receive a response from the endpoint"
    end

    response.headers.should eq NATS::Message::Headers{
      "one"  => "one",
      "many" => %w[one two],
    }
  end

  it "treats header keys as case-insensitive" do
    subject = "temp.#{UUID.random}"
    nats.subscribe subject do |msg|
      nats.reply msg, "", headers: NATS::Message::Headers{
        "CASE" => "insensitive",
      }
    end

    unless response = nats.request subject, ""
      raise "Did not receive a response from the endpoint"
    end

    response.headers.should eq NATS::Message::Headers{
      "case" => "insensitive",
    }
  end

  it "can set message headers with a reply-to" do
    subject = "temp.#{UUID.random}"
    headers = nil
    body = Bytes.empty
    reply_to = nil

    nats.subscribe subject do |msg|
      headers = msg.headers
      body = msg.body
      reply_to = msg.reply_to
    end

    nats.publish subject, "asdf", reply_to: "my-reply-to", headers: NATS::Message::Headers{"foo" => "bar"}
    nats.flush

    String.new(body).should eq "asdf"
    reply_to.should eq "my-reply-to"
    headers.should eq NATS::Message::Headers{"foo" => "bar"}
  end

  it "can publish to multiple subscribers" do
    subject = "temp.#{UUID.random}"

    count = 0
    2.times do
      nats.subscribe(subject) { count += 1 }
    end

    nats.publish subject, ""
    nats.flush # Flush the published message

    count.should eq 2
  end

  it "can unsubscribe from subjects" do
    subject = "temp.#{UUID.random}"
    count = 0

    subbed = nats.subscribe(subject) { count += 1 }
    unsubbed = nats.subscribe(subject) { count += 1 }

    # We subscribed, now we're gonna unsub from one of them
    nats.unsubscribe unsubbed

    nats.publish subject, ""
    nats.flush

    # Only one of the two `subscribe` blocks should have run
    count.should eq 1
  end

  it "can make requests and reply to those requests" do
    subject = "temp.#{UUID.random}"

    nats.subscribe subject do |msg|
      nats.reply msg, String.new(msg.body).upcase
    end

    if response = nats.request(subject, "foo")
      response.body.should eq "FOO".to_slice
    else
      raise "No response received"
    end
  end

  it "can handle multiple messages on the same subject concurrently" do
    subject = "temp.#{UUID.v7}"
    nats.subscribe subject, concurrency: 100 do |request, subscription|
      sleep 20.milliseconds
      nats.reply request, ""
    end

    start = Time.monotonic
    WaitGroup.wait do |wg|
      100.times do
        wg.spawn do
          unless response = nats.request subject
            raise "Did not receive a response from the endpoint"
          end
        end
      end
    end
    (Time.monotonic - start).should be_within 20.milliseconds, of: 20.milliseconds
  end

  describe "#request_many" do
    it "can make a request and receive many replies" do
      subject = "temp.#{UUID.random}"
      nats.subscribe subject do |msg|
        10.times do |i|
          nats.reply msg, i.to_s
        end
      end

      responses = nats.request_many subject, "", max_replies: 10, timeout: 1.second

      responses.size.should eq 10
    end

    it "can make a request and receive less than the specified number of replies" do
      subject = "temp.#{UUID.random}"
      nats.subscribe subject do |msg|
        9.times do |i|
          nats.reply msg, i.to_s
        end
      end

      responses = nats.request_many subject, "", max_replies: 10, timeout: 50.milliseconds

      responses.size.should eq 9
    end

    it "can make a request and receive many replies spaced out over time" do
      subject = "temp.#{UUID.random}"
      nats.subscribe subject do |msg|
        3.times do |i|
          nats.reply msg, i.to_s
          sleep 50.milliseconds
        end
      end

      # We ask for up to 10, but we only wait long enough to get 2 because they
      # come in 50ms apart. We could reduce the duration to keep the test suite
      # fast, but variances in VM performance would make the test unpredictable.
      responses = nats.request_many subject, "",
        max_replies: 10,
        timeout: 100.milliseconds

      responses.size.should eq 2
    end

    it "raises when passing a negative max_replies" do
      expect_raises ArgumentError do
        nats.request_many "asdf", max_replies: -1
      end
    end

    it "can make a request and receive an unbounded number of messages until N seconds have passed without a message" do
      subject = "temp.#{UUID.random}"
      nats.subscribe subject do |msg|
        9.times do |i|
          nats.reply msg, i.to_s
        end
        sleep 100.milliseconds
        nats.reply msg, "this is never received"
      end

      responses = nats.request_many subject, stall_timeout: 50.milliseconds

      responses.size.should eq 9
    end

    it "raises when passing a negative stall_timeout" do
      expect_raises ArgumentError do
        nats.request_many "asdf", stall_timeout: -1.second
      end
    end

    it "can make a request for many responses with a block determining whether to continue waiting" do
      subject = "temp.#{UUID.random}"
      nats.subscribe subject do |msg|
        10.times do |i|
          nats.reply msg, i.to_s
        end
        nats.reply msg, headers: NATS::Headers{"stop" => "true"}
      end

      responses = nats.request_many subject do |response|
        response.headers["stop"]?
      end

      responses.size.should eq 10
    end
  end

  it "assigns replies to the original requesters" do
    subject = "temp.#{UUID.random}"
    # Echoing requests back to their requesters
    nats.subscribe subject do |msg|
      sleep rand.microseconds
      nats.reply msg, msg.body
    end

    channel = Channel(Nil).new
    replies = Array.new(100) { -1 }
    100.times do |i|
      spawn do
        if response = nats.request(subject, i.to_s)
          replies[i] = String.new(response.body).to_i
        end
      ensure
        channel.send nil
      end
    end
    100.times { channel.receive }

    replies.should eq Array.new(100, &.itself)
  end

  it "receives a reply from only a single subscriber" do
    subject = "temp.#{UUID.random}"

    10.times do |i|
      nats.subscribe subject do |msg|
        nats.reply msg, i.to_s
      end
    end

    response = nats.request(subject, "") || raise "no response"
    # We have no guarantee which subscriber responds first, so we'll just make
    # sure that *one* of them did.
    (0...10).should contain String.new(response.body).to_i
  end

  it "receives a single reply when requested asynchronously with a block" do
    subject = "temp.#{UUID.random}"
    count = 0

    10.times do |i|
      nats.subscribe subject do |msg|
        nats.reply msg, i.to_s
      end
    end

    nats.request(subject, "") do |response|
      count += 1
    end
    nats.flush              # Send the request
    nats.flush              # Give the response time to come back
    10.times { nats.flush } # Just to see if we're gonna get back any more

    count.should eq 1
  end

  it "returns early when there are no responders" do
    subject = "temp.#{UUID.random}"
    called = false

    start = Time.monotonic
    response = nats.request(subject, "", timeout: 1.second)
    finish = Time.monotonic

    called.should eq false
    response.should eq nil
    ((finish - start) < 1.second).should eq true
  end

  it "handles disconnects" do
    subject = "temp.#{UUID.random}"
    nats.@socket.close # OOPS WE BROKE THE INTERNET
    data = nil

    nats.subscribe subject do |msg|
      data = msg.body
    end
    sleep 100.milliseconds # Allow for reconnect

    nats.publish subject, "yep"
    nats.flush

    data.should eq "yep".to_slice
  end

  it "buffers data after a disconnect and sends it upon reconnection" do
    subject = "temp.#{UUID.random}"
    data = nil

    nats.subscribe subject do |msg|
      data = msg.body
    end
    nats.@socket.close     # OOPS WE BROKE THE INTERNET
    sleep 100.milliseconds # Allow time to reconnect

    nats.publish subject, "yep"
    nats.flush

    data.should eq "yep".to_slice
  end

  it "drains subscriptions" do
    n = NATS::Client.new
    begin
      first = UUID.random.to_s
      second = UUID.random.to_s
      msgs = Hash(String, Array(String)).new { |h, k| h[k] = [] of String }
      n.subscribe first do |msg|
        msgs[first] << String.new(msg.body)
      end
      n.subscribe second do |msg|
        msgs[second] << String.new(msg.body)
      end
      n.publish first, "one"
      n.publish first, "two"
      n.publish second, "1"
      n.publish second, "2"

      n.drain

      msgs.should eq({
        first  => %w[one two],
        second => %w[1 2],
      })
    ensure
      n.close
    end
  end

  it "drains subscriptions before closing" do
    n = NATS::Client.new
    subject = UUID.random.to_s
    greeting = nil
    n.subscribe(subject) do |msg|
      sleep 1.millisecond
      greeting = String.new(msg.body)
    end
    n.flush
    10.times { |i| n.publish subject, "hi #{i}" }

    n.close

    # 10 messages, 0..9
    greeting.should eq "hi 9"
  end

  it "connects to a server using NKeys" do
    port = rand(50_000..60_000)
    subject = UUID.random.to_s

    run_nats_on port, config: "nats_nkeys" do |client|
      client.subscribe(subject) { |msg| client.reply msg, String.new(msg.body).upcase }

      response = client.request(subject, "hello").not_nil!
      response.body.should eq "HELLO".to_slice
    end
  end

  it "reconnects if we hit too many pings" do
    fake_nats_server = TCPServer.new("127.0.0.1", port: 0)
    spawn do
      until fake_nats_server.closed?
        if client = fake_nats_server.accept?
          client << "INFO "
          {
            server_id:   "lol",
            server_name: "fake",
            version:     "1.2.3.4",
            go:          "fish",
            host:        "0.0.0.0",
            port:        0,
            headers:     true,
            max_payload: 2**20,
            proto:       1,
            client_id:   Random::Secure.rand(UInt64),
          }.to_json client
          client << "\r\n"

          # Read the CONNECT line
          client.read_line.should start_with "CONNECT "
          # Read the initial ping
          client.read_line.should eq "PING"
          # Respond to the initial ping to complete the handshake
          client << "PONG\r\n"
        end
      end
    end

    n = NATS::Client.new(
      uri: URI.parse("nats://#{fake_nats_server.local_address.address}:#{fake_nats_server.local_address.port}"),
      # The fake NATS server does not respond to pings beyond the initial one
      # that completes the NATS connection, so we should be disconnected after
      # the third ping is sent out at (50ms + latency) * 3 due to having too
      # many outstanding pings
      ping_interval: 20.milliseconds,
      max_pings_out: 2,
    )
    disconnected = false
    n.on_disconnect { disconnected = true }
    pings = 0
    n.on_ping do
      pings += 1
    end

    begin
      sleep 50.milliseconds
      # We have only send out 2 unanswered pings at this point, so we should
      # not have tried to reconnect
      disconnected.should eq false
      pings.should eq 2

      sleep 40.milliseconds
      # At this point, we should have sent too many unanswered pings and
      # been disconnected.
      disconnected.should eq true
      pings.should eq 4
    ensure
      fake_nats_server.close
    end
  end
end

private def run_nats_on(port : Int32, config : String, &)
  Process.run "nats-server", args: "-js --port #{port} --config #{__DIR__}/support/#{config}.conf".split do |process|
    wait_for_nats port
    client = NATS::Client.new(
      uri: URI.parse("nats://:#{port}"),
      nkeys_file: "#{__DIR__}/support/#{config}.seed",
    )
    yield client
  ensure
    process.signal :term
  end
end

private def wait_for_nats(port : Int32, host : String = "127.0.0.1")
  loop do
    if socket = TCPSocket.new(host, port)
      socket.close
      return
    end
  rescue ex : IO::Error
    sleep 10.milliseconds
  end
end
