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

  it "publishes to streams and reads with push consumers" do
    write_subject = UUID.random.to_s
    stream = create_stream([write_subject])

    consumer_name = UUID.random.to_s
    read_subject = UUID.random.to_s
    deliver_group = UUID.random.to_s

    consumer = nats.jetstream.consumer.create(
      stream_name: stream.config.name,
      durable_name: consumer_name,
      deliver_group: deliver_group,
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

  it "reads from pull consumers" do
    write_subject = UUID.random.to_s
    stream = create_stream([write_subject])

    consumer_name = UUID.random.to_s
    deliver_group = UUID.random.to_s

    consumer = nats.jetstream.consumer.create(
      stream_name: stream.config.name,
      durable_name: consumer_name,
      deliver_group: deliver_group,
    )

    pull = nats.jetstream.pull_subscribe(consumer)
    # 50.times { nats.jetstream.pull_subscribe(consumer) }

    begin
      spawn do
        3.times { |i| nats.publish write_subject, i.to_s }
        nats.flush
      end

      if msg = pull.fetch(timeout: 2.seconds)
        msg.subject.should eq write_subject
        msg.body.should eq "0".to_slice

        nats.jetstream.ack msg
      else
        raise "Did not receive a message within 2 seconds"
      end

      # Fetch the remaining 2 messages, but wait up to 500ms for a 3rd. We're
      # specifying a short timeout because we know there is no 3rd message
      # (we've already consumed 1 of 3 total messages, so there are only 2 left)
      # and we don't want to wait the full 2 seconds for it to timeout. Plus,
      # it also lets us exercise the timeout functionality.
      msgs = pull.fetch(3, timeout: 500.milliseconds)
      msgs.map(&.body).should eq [
        "1".to_slice,
        "2".to_slice,
      ]

      msgs.each { |msg| nats.jetstream.ack msg }
      nats.flush
    ensure
      nats.jetstream.stream.delete stream
    end
  end
end
