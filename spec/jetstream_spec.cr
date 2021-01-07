require "./spec_helper"
require "../src/jetstream"
require "uuid"

private macro create_stream(subjects)
  nats.jetstream.stream.create(
    name: "test-stream-#{UUID.random}",
    subjects: {{subjects}},
    storage: :memory,
  )
end

nats = NATS::Client.new

describe NATS::JetStream do
  it "creates and deletes streams" do
    stream = nats.jetstream.stream.create(
      name: "test-stream-#{UUID.random}",
      subjects: %w[test.jetstream.#{UUID.random}.*],
      storage: :memory,
    )

    begin
      stream.config.name.should match /test-stream-[[:xdigit:]]{8}(-[[:xdigit:]]{4}){3}-[[:xdigit:]]{12}/
      nats.jetstream.stream.list.streams.should contain stream
    ensure
      nats.jetstream.stream.delete stream

      nats.jetstream.stream.list.streams.should_not contain stream
    end
  end

  it "creates and deletes consumers" do
    subjects = [UUID.random.to_s]
    stream = create_stream(subjects)

    begin
    ensure
      nats.jetstream.stream.delete stream
      nats.jetstream.stream.list.streams.should_not contain stream
    end
  end

  it "publishes to streams and reads from consumers" do
    write_subject = UUID.random.to_s
    stream = create_stream([write_subject])

    consumer_name = UUID.random.to_s
    read_subject = UUID.random.to_s
    consumer = nats.jetstream.consumer.create(
      stream_name: stream.config.name,
      durable_name: consumer_name,
      deliver_subject: read_subject,
    )

    begin
      nats.publish write_subject, "hello"
      channel = Channel(Nil).new

      msg_subject = nil
      msg_body = nil
      nats.subscribe read_subject do |msg|
        msg_subject = msg.subject
        msg_body = msg.body
        nats.reply msg, ""
        channel.send nil
      end

      select
      when channel.receive
      when timeout(2.seconds)
        raise "Did not receive message within 2 seconds"
      end

      msg_subject.should eq write_subject
      msg_body.should eq "hello".to_slice
    ensure
      nats.jetstream.stream.delete stream
    end
  end
end
