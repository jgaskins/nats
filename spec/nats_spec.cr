require "./spec_helper"
require "../src/nats"
require "uuid"

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
    nats.@socket.close # OOPS WE BROKE THE INTERNET
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
        first => %w[one two],
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
end

private def run_nats_on(port : Int32, config : String)
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
