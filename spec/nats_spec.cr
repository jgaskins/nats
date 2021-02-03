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

    nats.publish subject, "asdf", headers: NATS::Message::Headers { "foo" => "bar" }
    nats.flush

    String.new(body).should eq "asdf"
    headers.should eq NATS::Message::Headers { "foo" => "bar" }
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

    nats.publish subject, "asdf", reply_to: "my-reply-to", headers: NATS::Message::Headers { "foo" => "bar" }
    nats.flush

    String.new(body).should eq "asdf"
    reply_to.should eq "my-reply-to"
    headers.should eq NATS::Message::Headers { "foo" => "bar" }
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

  it "receives a reply from only a single subscriber" do
    subject = "temp.#{UUID.random}"

    10.times do |i|
      nats.subscribe subject do |msg|
        nats.reply msg, i.to_s
      end
    end

    if response = nats.request(subject, "")
      # We have no guarantee which subscriber responds first, so we'll just make
      # sure that *one* of them did.
      (0...10).should contain String.new(response.body).to_i
    else
      no_response!
    end
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
    nats.flush # Send the request
    nats.flush # Give the response time to come back
    10.times { nats.flush } # Just to see if we're gonna get back any more

    count.should eq 1
  end

  it "handles disconnects" do
    subject = "temp.#{UUID.random}"
    nats.@socket.close # OOPS WE BROKE THE INTERNET
    data = nil

    nats.subscribe subject do |msg|
      data = msg.body
    end

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

    nats.publish subject, "yep"
    nats.flush

    data.should eq "yep".to_slice
  end
end

private def no_response!
  raise "No response received"
end
