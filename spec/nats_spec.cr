require "./spec_helper"
require "./support/fake_nats_server"
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

  it "raises NATS::Error if the message payload is too large for the server" do
    expect_raises NATS::Error do
      nats.publish "foo", "a" * (nats.server_info.max_payload + 1)
    end
  end

  it "raises NATS::Error if the message payload and headers combined are too large for the server" do
    expect_raises NATS::Error do
      nats.publish "foo", "a" * (nats.server_info.max_payload - 10),
        headers: NATS::Headers{"extra" => "trying to exceed the limit"}
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

    start = Time.instant
    WaitGroup.wait do |wg|
      100.times do
        wg.spawn do
          unless response = nats.request subject
            raise "Did not receive a response from the endpoint"
          end
        end
      end
    end
    start.elapsed.should be_within 20.milliseconds, of: 20.milliseconds
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

    it "can make many requests and receive a reply for each one" do
      subjects = Array.new(10) { "temp.#{UUID.v4}" }
      subjects.each_with_index do |subject, index|
        nats.subscribe subject do |msg|
          # Make each reply take a different amount of time to avoid a situation
          # where the messages are returned in the same order by accident.
          sleep (10 - index).milliseconds
          nats.reply msg, subject
        end
      end
      msgs = subjects.map do |subject|
        NATS::Message.new(
          subject: subject,
          data: "",
        )
      end

      responses = nats.request_many msgs

      # The messages must be returned in the order in which the original
      # requests were sent.
      responses.map(&.try(&.data_string)).should eq subjects
    end

    it "can make many requests and receive a reply for some of them" do
      subjects = Array.new(10) { "temp.#{UUID.v4}" }
      subjects.each_with_index do |subject, index|
        nats.subscribe subject do |msg|
          # Some of the messages (in this case, the second half of them) take
          # longer than the timeout on purpose. We expect not to get replies
          # for those.
          if index >= 5
            sleep 100.milliseconds
          end
          nats.reply msg, subject
        end
      end
      msgs = subjects.map do |subject|
        NATS::Message.new(
          subject: subject,
          data: "",
        )
      end

      responses = nats.request_many msgs, timeout: 10.milliseconds

      # We expect to receive the first 5 messages, but we timeout far too
      # quickly (10ms) to receive the second half (which each take 100ms).
      responses.map(&.try(&.data_string)).should eq subjects[0...5] + [nil] * 5
    end

    it "can make a request and receive less than the specified number of replies" do
      subject = "temp.#{UUID.random}"
      nats.subscribe subject do |msg|
        9.times do |i|
          nats.reply msg, i.to_s
        end
      end

      responses = nats.request_many subject,
        max_replies: 10,
        timeout: 50.milliseconds

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
        # Our sentinel message has a "stop" header
        nats.reply msg, headers: NATS::Headers{"stop" => "true"}
      end

      responses = [] of NATS::Message
      nats.request_many subject do |response|
        break if response.headers["stop"]?
        responses << response
      end

      # Sentinel message is not included in the response array
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

    start = Time.instant
    response = nats.request(subject, "", timeout: 1.second)

    called.should eq false
    response.should eq nil
    start.elapsed.should be < 1.second
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

  describe "connection handling" do
    it "reconnects after too many unanswered pings and then stays connected" do
      server = FakeNATSServer.new
      # Only the handshake PING gets a PONG until we say otherwise
      server.respond_to_pings = false
      n = NATS::Client.new(
        uri: server.uri,
        ping_interval: 20.milliseconds,
        max_pings_out: 2,
      )
      disconnects = 0
      pings = 0
      n.on_ping { pings += 1 }
      n.on_disconnect do
        disconnects += 1
        # Behave properly on the new connection
        server.respond_to_pings = true
      end

      begin
        server.wait_for_connections 2
        # We only disconnect once we have more than max_pings_out unanswered
        pings.should be >= 3
        disconnects.should eq 1
        wait_for(description: "reconnect") { n.state.connected? }

        # Now that pings are answered, we should stay connected for many ping
        # intervals without reconnecting again.
        sleep 200.milliseconds
        n.state.connected?.should eq true
        disconnects.should eq 1
        server.connection_count.should eq 2
        n.flush timeout: 1.second
      ensure
        n.close
        server.close
      end
    end

    it "does not block publishing while disconnected and delivers buffered data after reconnecting" do
      server = FakeNATSServer.new
      n = NATS::Client.new(uri: server.uri)
      received = Channel(String).new(10)
      disconnects = 0
      reconnects = 0
      states_on_disconnect = [] of NATS::Client::State
      n.on_disconnect { disconnects += 1; states_on_disconnect << n.state }
      n.on_reconnect { reconnects += 1 }
      subscription = n.subscribe("test.#{UUID.random}") { |msg| received.send msg.data_string }
      n.flush

      begin
        # Take the server down
        server.accepting = false
        server.drop_connections
        wait_for(description: "disconnect") { !n.state.connected? }

        start = Time.instant
        n.publish subscription.subject, "buffered while down"
        start.elapsed.should be < 100.milliseconds

        # Requests still honor their timeout instead of hanging until the
        # server comes back
        start = Time.instant
        n.request("nobody.home", "hi", timeout: 50.milliseconds).should eq nil
        start.elapsed.should be < 500.milliseconds

        # Bring the server back
        server.accepting = true
        wait_for(description: "reconnect") { n.state.connected? }

        select
        when data = received.receive
          data.should eq "buffered while down"
        when timeout(2.seconds)
          fail "Buffered message was not delivered after reconnecting"
        end

        # The subscription object the caller holds is still the live one
        n.@subscriptions[subscription.sid].should be subscription
        n.publish subscription.subject, "after reconnect"
        n.flush
        select
        when data = received.receive
          data.should eq "after reconnect"
        when timeout(2.seconds)
          fail "Message was not delivered on the new connection"
        end

        disconnects.should eq 1
        reconnects.should eq 1
        states_on_disconnect.should eq [NATS::Client::State::Disconnected]
      ensure
        n.close
        server.close
      end
    end

    it "re-establishes subscriptions with their remaining message limits after reconnecting" do
      server = FakeNATSServer.new
      n = NATS::Client.new(uri: server.uri)
      received = [] of String
      subject = "limited.#{UUID.random}"
      subscription = n.subscribe(subject) { |msg| received << msg.data_string }
      n.unsubscribe subscription, 2
      n.publish subject, "one"
      n.flush
      wait_for { received.size == 1 }

      begin
        server.drop_connections
        wait_for(description: "reconnect") { server.connection_count == 2 && n.state.connected? }

        n.publish subject, "two"
        n.publish subject, "three"
        n.flush
        sleep 20.milliseconds

        received.should eq %w[one two]
      ensure
        n.close
        server.close
      end
    end

    it "raises when the reconnect buffer is exceeded and keeps what was buffered" do
      server = FakeNATSServer.new
      n = NATS::Client.new(uri: server.uri, reconnect_buffer_size: 64)
      received = Channel(String).new(10)
      subscription = n.subscribe("test.#{UUID.random}") { |msg| received.send msg.data_string }
      n.flush

      begin
        server.accepting = false
        server.drop_connections
        wait_for(description: "disconnect") { !n.state.connected? }

        n.publish subscription.subject, "x" * 100
        expect_raises NATS::Client::ReconnectBufferExceeded do
          n.publish subscription.subject, "y" * 100
        end

        server.accepting = true
        wait_for(description: "reconnect") { n.state.connected? }
        select
        when data = received.receive
          data.should eq "x" * 100
        when timeout(2.seconds)
          fail "Buffered message was not delivered after reconnecting"
        end
      ensure
        n.close
        server.close
      end
    end

    it "fails a pending flush when the connection drops instead of waiting for the timeout" do
      server = FakeNATSServer.new
      server.respond_to_pings = false
      n = NATS::Client.new(uri: server.uri)
      result = Channel(Exception?).new(1)

      begin
        spawn do
          n.flush timeout: 10.seconds
          result.send nil
        rescue ex
          result.send ex
        end
        sleep 10.milliseconds
        start = Time.instant
        server.drop_connections

        select
        when ex = result.receive
          ex.should be_a NATS::Error
          ex.not_nil!.message.to_s.should contain "lost"
          start.elapsed.should be < 1.second
        when timeout(2.seconds)
          fail "flush did not fail when the connection dropped"
        end
      ensure
        n.close
        server.close
      end
    end

    it "closes promptly while disconnected and stops trying to reconnect" do
      server = FakeNATSServer.new
      n = NATS::Client.new(uri: server.uri)
      disconnects = 0
      n.on_disconnect { disconnects += 1 }

      begin
        server.accepting = false
        server.drop_connections
        wait_for(description: "disconnect") { !n.state.connected? }

        start = Time.instant
        n.close
        start.elapsed.should be < 100.milliseconds
        n.state.closed?.should eq true

        # Even once the server is back, a closed client stays closed and
        # doesn't fire on_disconnect for writes that fail because it's closed.
        server.accepting = true
        sleep 100.milliseconds
        n.state.closed?.should eq true
        server.connections.should be_empty
        expect_raises(NATS::Client::ClientClosed) { n.publish "foo", "bar" }
        disconnects.should eq 1
      ensure
        n.close
        server.close
      end
    end

    it "recovers from a protocol error by reconnecting" do
      server = FakeNATSServer.new
      n = NATS::Client.new(uri: server.uri)
      errors = [] of Exception
      n.on_error { |ex| errors << ex }
      n.flush

      begin
        # A MSG line we can't parse leaves us in an unknown place in the
        # stream, so the client should drop the connection and start over
        # instead of dying.
        server.connections.first.send "MSG this-is-not-valid"
        wait_for(description: "reconnect") { server.connection_count == 2 && n.state.connected? }

        errors.map(&.class).should contain NATS::Error
        n.flush timeout: 1.second
      ensure
        n.close
        server.close
      end
    end

    it "does not let an on_error handler that raises kill the client" do
      server = FakeNATSServer.new
      n = NATS::Client.new(uri: server.uri)
      n.on_error { |ex| raise "on_error handler blew up" }
      n.flush

      begin
        server.connections.first.send "BOGUS"
        sleep 10.milliseconds
        # The client is still alive and connected
        n.flush timeout: 1.second
        n.state.connected?.should eq true
      ensure
        n.close
        server.close
      end
    end

    describe "lame-duck mode" do
      it "notifies the application once when the server enters lame-duck mode" do
        server = FakeNATSServer.new
        n = NATS::Client.new(uri: server.uri)
        notifications = 0
        n.on_lame_duck_mode { notifications += 1 }
        n.flush

        begin
          server.connections.first.send_info ldm: true
          wait_for(description: "lame-duck notification") { notifications == 1 }
          n.server_info.lame_duck_mode?.should eq true

          # Further INFO updates while still in lame-duck mode don't notify again
          server.connections.first.send_info ldm: true
          n.flush timeout: 1.second
          sleep 10.milliseconds
          notifications.should eq 1
          n.state.connected?.should eq true
        ensure
          n.close
          server.close
        end
      end

      it "moves to a server from connect_urls when the lame-duck server closes the connection" do
        lame_duck = FakeNATSServer.new
        other = FakeNATSServer.new
        n = NATS::Client.new(uri: lame_duck.uri)
        received = Channel(String).new(10)
        subject = "ldm.#{UUID.random}"
        n.subscribe(subject) { |msg| received.send msg.data_string }
        n.flush

        begin
          lame_duck.connections.first.send_info ldm: true, connect_urls: ["127.0.0.1:#{other.port}"]
          wait_for(description: "lame-duck INFO") { n.server_info.lame_duck_mode? }
          # A lame-duck server stops accepting connections before it closes
          # the ones it has
          lame_duck.accepting = false
          lame_duck.drop_connections

          wait_for(description: "reconnect to the other server") { other.connections.any? && n.state.connected? }
          n.server_info.lame_duck_mode?.should eq false
          n.publish subject, "hello"
          select
          when data = received.receive
            data.should eq "hello"
          when timeout(2.seconds)
            fail "Subscription was not re-established on the other server"
          end
        ensure
          n.close
          lame_duck.close
          other.close
        end
      end

      it "keeps credentials and the connection name when connecting to a discovered server" do
        lame_duck = FakeNATSServer.new
        other = FakeNATSServer.new
        n = NATS::Client.new(uri: URI.parse("nats://user:pass@127.0.0.1:#{lame_duck.port}/my-app"))
        n.flush

        begin
          lame_duck.connections.first.send_info ldm: true, connect_urls: ["127.0.0.1:#{other.port}"]
          wait_for(description: "lame-duck INFO") { n.server_info.lame_duck_mode? }
          lame_duck.accepting = false
          lame_duck.drop_connections

          wait_for(description: "reconnect to the other server") { other.connections.any?(&.connect_options) }
          options = other.connections.first.connect_options.not_nil!
          options["user"].should eq "user"
          options["pass"].should eq "pass"
          options["name"].should eq "my-app"
        ensure
          n.close
          lame_duck.close
          other.close
        end
      end
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
