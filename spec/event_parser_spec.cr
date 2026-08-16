require "./spec_helper"
require "../src/event_parser"

describe NATS::EventParser do
  it "parses a message without headers" do
    io = IO::Memory.new(<<-EOF)
      MSG subject sid reply_to 11\r
      hello world\r

      EOF
    event = NATS::EventParser.new(io).call.should be_a NATS::Protocol::MessageEvent

    msg = event.message
    msg.subject.should eq "subject"
    msg.reply_to.should eq "reply_to"
    msg.data_string.should eq "hello world"
    event.sid.should eq "sid"
  end

  it "parses a message with headers" do
    io = IO::Memory.new(<<-EOF)
      HMSG subject sid reply_to 27 38\r
      NATS/1.0\r
      Header: Value\r
      \r
      hello world\r

      EOF
    event = NATS::EventParser.new(io).call.should be_a NATS::Protocol::MessageEvent

    msg = event.message
    msg.subject.should eq "subject"
    msg.reply_to.should eq "reply_to"
    msg.data_string.should eq "hello world"
    msg.headers.should eq NATS::Headers{"Header" => "Value"}
    event.sid.should eq "sid"
  end

  it "parses a message with no reply_to" do
    io = IO::Memory.new(<<-EOF)
      MSG subject sid 11\r
      hello world\r

      EOF
    event = NATS::EventParser.new(io).call.should be_a NATS::Protocol::MessageEvent

    msg = event.message
    msg.subject.should eq "subject"
    msg.reply_to.should be_nil
    msg.data_string.should eq "hello world"
    event.sid.should eq "sid"
  end

  it "parses a message with headers and no reply_to" do
    io = IO::Memory.new(<<-EOF)
      HMSG subject sid 27 38\r
      NATS/1.0\r
      Header: Value\r
      \r
      hello world\r

      EOF
    event = NATS::EventParser.new(io).call.should be_a NATS::Protocol::MessageEvent

    msg = event.message
    msg.subject.should eq "subject"
    msg.reply_to.should be_nil
    msg.data_string.should eq "hello world"
    msg.headers.should eq NATS::Headers{"Header" => "Value"}
    event.sid.should eq "sid"
  end

  it "parses a message whose header block holds no headers" do
    io = IO::Memory.new(<<-EOF)
      HMSG subject sid reply_to 12 23\r
      NATS/1.0\r
      \r
      hello world\r

      EOF
    event = NATS::EventParser.new(io).call.should be_a NATS::Protocol::MessageEvent

    msg = event.message
    msg.data_string.should eq "hello world"
    msg.headers.should be_empty
    msg.headers["Status"]?.should be_nil
  end

  it "parses a message with several headers, including repeated ones" do
    io = IO::Memory.new(<<-EOF)
      HMSG subject sid reply_to 43 54\r
      NATS/1.0\r
      First: 1\r
      Second: 2\r
      First: 3\r
      \r
      hello world\r

      EOF
    event = NATS::EventParser.new(io).call.should be_a NATS::Protocol::MessageEvent

    msg = event.message
    msg.data_string.should eq "hello world"
    msg.headers.get("First").should eq %w[1 3]
    msg.headers.get("Second").should eq %w[2]
  end

  it "exposes the status of a no-responders reply" do
    io = IO::Memory.new(<<-EOF)
      HMSG _INBOX.wLQZbaDX 3 30 30\r
      NATS/1.0 503 No Responders\r
      \r
      \r

      EOF
    event = NATS::EventParser.new(io).call.should be_a NATS::Protocol::MessageEvent

    msg = event.message
    msg.subject.should eq "_INBOX.wLQZbaDX"
    msg.data_string.should eq ""
    msg.headers["Status"].should eq "503 No Responders"
    event.sid.should eq "3"
  end

  it "exposes a status with no description" do
    io = IO::Memory.new(<<-EOF)
      HMSG _INBOX.wLQZbaDX 3 16 16\r
      NATS/1.0 100\r
      \r
      \r

      EOF
    event = NATS::EventParser.new(io).call.should be_a NATS::Protocol::MessageEvent

    event.message.headers["Status"].should eq "100"
  end

  it "consumes exactly one event, leaving the next one on the IO" do
    io = IO::Memory.new(<<-EOF)
      MSG subject sid 5\r
      first\r
      HMSG subject sid reply_to 27 33\r
      NATS/1.0\r
      Header: Value\r
      \r
      second\r

      EOF
    parser = NATS::EventParser.new(io)

    first = parser.call.should(be_a NATS::Protocol::MessageEvent).message
    first.data_string.should eq "first"
    first.headers.should be_empty

    second = parser.call.should(be_a NATS::Protocol::MessageEvent).message
    second.data_string.should eq "second"
    second.headers.should eq NATS::Headers{"Header" => "Value"}

    io.pos.should eq io.size
  end

  it "parses PING and PONG" do
    io = IO::Memory.new("PING\r\nPONG\r\n")
    parser = NATS::EventParser.new(io)

    parser.call.should be_a NATS::Protocol::PingEvent
    parser.call.should be_a NATS::Protocol::PongEvent
    io.pos.should eq io.size
  end

  it "parses +OK" do
    io = IO::Memory.new("+OK\r\n")

    NATS::EventParser.new(io).call.should be_a NATS::Protocol::OKEvent
  end

  it "parses INFO" do
    io = IO::Memory.new(<<-EOF)
      INFO {"server_id":"NABC","server_name":"nats-1","version":"2.10.7","go":"go1.21.4","host":"0.0.0.0","port":4222,"headers":true,"max_payload":1048576,"proto":1,"connect_urls":["nats-2:4222","nats-3:4222"]}\r

      EOF
    event = NATS::EventParser.new(io).call.should be_a NATS::Protocol::InfoEvent

    event.server_info.server_name.should eq "nats-1"
    event.server_info.max_payload.should eq 1_048_576
    event.server_info.connect_urls.should eq %w[nats-2:4222 nats-3:4222]
  end

  it "parses -ERR" do
    io = IO::Memory.new("-ERR 'Authorization Violation'\r\n")
    event = NATS::EventParser.new(io).call.should be_a NATS::Protocol::ErrorEvent

    event.error.message.should eq "-ERR 'Authorization Violation'"
  end

  it "reports commands it doesn't recognize" do
    {"WAT is this\r\n", "PINGER\r\n", "+NOPE\r\n", "INFORMAL\r\n"}.each do |line|
      event = NATS::EventParser.new(IO::Memory.new(line)).call
        .should be_a NATS::Protocol::UnknownEvent

      event.error.should be_a NATS::UnknownCommand
      event.error.message.should eq line.chomp
    end
  end

  it "consumes exactly one control line, leaving the next event on the IO" do
    io = IO::Memory.new(<<-EOF)
      PING\r
      MSG subject sid 11\r
      hello world\r

      EOF
    parser = NATS::EventParser.new(io)

    parser.call.should be_a NATS::Protocol::PingEvent
    parser.call.should(be_a NATS::Protocol::MessageEvent)
      .message.data_string.should eq "hello world"
    io.pos.should eq io.size
  end
end
