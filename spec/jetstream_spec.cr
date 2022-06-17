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

private macro create_consumer(stream)
  nats.jetstream.consumer.create(
    stream_name: stream.config.name,
    durable_name: UUID.random.to_s,
    deliver_group: UUID.random.to_s,
    deliver_subject: UUID.random.to_s,
  )
end

nats = NATS::Client.new
js = nats.jetstream

describe NATS::JetStream do
  it "creates, lists, and deletes streams" do
    uuid = UUID.random
    included = nats.jetstream.stream.create(
      name: "included-#{uuid}",
      subjects: ["test.jetstream.included.#{uuid}.*"],
      storage: :memory,
    )
    excluded = nats.jetstream.stream.create(
      name: "excluded-#{uuid}",
      subjects: ["test.jetstream.excluded.#{uuid}.*"],
      storage: :memory,
    )

    begin
      included.config.name.should eq "included-#{uuid}"
      all = nats.jetstream.stream.list.streams
      filtered = nats.jetstream.stream.list(subject: "test.jetstream.included.#{uuid}.*").streams
      all.should contain included
      all.should contain excluded

      filtered.should contain included
      filtered.should_not contain excluded
    ensure
      nats.jetstream.stream.delete included
      nats.jetstream.stream.delete excluded

      nats.jetstream.stream.list.streams.should_not contain included
      nats.jetstream.stream.list.streams.should_not contain excluded
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
      js.publish write_subject, "hello"
      channel = Channel(Nil).new

      msg_subject = nil
      msg_body = nil
      nats.jetstream.subscribe consumer do |msg|
        msg_subject = msg.subject
        msg_body = msg.raw_data
        js.ack msg
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

  describe "acknowledging" do
    it "acknowledges messages" do
      write_subject = UUID.random.to_s
      stream = create_stream([write_subject])
      consumer = create_consumer(stream)
      channel = Channel(NATS::JetStream::Message).new
      nats.jetstream.publish write_subject, "omg"

      begin
        nats.jetstream.subscribe consumer do |msg|
          nats.jetstream.ack msg
          nats.flush
          channel.send msg
          channel.close
        end

        msg = channel.receive
        consumer = nats.jetstream.consumer.info!(consumer.stream_name, consumer.name)
        consumer.ack_floor.consumer_seq.should eq 1
      ensure
        nats.jetstream.stream.delete stream
      end
    end

    it "can wait for double-acknowledgement from the server" do
      write_subject = UUID.random.to_s
      stream = create_stream([write_subject])
      consumer = create_consumer(stream)
      nats.jetstream.publish write_subject, "omg"
      channel = Channel(NATS::JetStream::Message).new

      begin
        nats.jetstream.subscribe consumer do |msg|
          nats.jetstream.ack_sync msg
          channel.send msg
        end

        msg = channel.receive
        consumer = nats.jetstream.consumer.info!(consumer.stream_name, consumer.name)
        consumer.ack_floor.consumer_seq.should eq 1
      ensure
        nats.jetstream.stream.delete stream
      end
    end

    it "can negatively acknowledge (reject) a message" do
      write_subject = UUID.random.to_s
      stream = create_stream([write_subject])
      consumer = create_consumer(stream)
      nats.jetstream.publish write_subject, "omg"
      channel = Channel(NATS::JetStream::Message).new

      begin
        nats.jetstream.subscribe consumer do |msg|
          nats.jetstream.nack msg
          nats.flush
          channel.send msg
        end

        msg = channel.receive
        consumer = nats.jetstream.consumer.info!(consumer.stream_name, consumer.name)
        consumer.ack_floor.consumer_seq.should eq 0
        consumer.num_ack_pending.should eq 1
        consumer.num_redelivered.should eq 1
      ensure
        nats.jetstream.stream.delete stream
      end
    end

    it "can tell the server it needs more time with a WIP ack" do
      write_subject = UUID.random.to_s
      stream = create_stream([write_subject])
      consumer = create_consumer(stream)
      nats.jetstream.publish write_subject, "omg"
      channel = Channel(NATS::JetStream::Message).new

      begin
        nats.jetstream.subscribe consumer do |msg|
          nats.jetstream.in_progress msg
          nats.flush
          channel.send msg
        end

        msg = channel.receive
        consumer = nats.jetstream.consumer.info!(consumer.stream_name, consumer.name)
        consumer.ack_floor.consumer_seq.should eq 0
        consumer.num_ack_pending.should eq 1
        consumer.num_redelivered.should eq 0
      ensure
        nats.jetstream.stream.delete stream
      end
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

    begin
      spawn do
        3.times { |i| nats.publish write_subject, i.to_s }
        nats.flush
      end

      if msg = pull.fetch(timeout: 2.seconds)
        msg.subject.should eq write_subject
        msg.raw_data.should eq "0".to_slice

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
      msgs.map(&.raw_data).should eq [
        "1".to_slice,
        "2".to_slice,
      ]

      msgs.each { |msg| nats.jetstream.ack msg }
      nats.flush
    ensure
      nats.jetstream.stream.delete stream
    end
  end

  describe "purging a stream" do
    it "purges by subject" do
      first, second = Array.new(2) { UUID.random.to_s }
      stream = create_stream(subjects: [first, second])
      name = stream.config.name

      js.publish first, "test"
      js.publish first, "another test"
      js.publish second, "yet another"

      seq_1_msg = js.stream.get_msg(name, sequence: 1).not_nil!.message
      seq_1_msg.data.should eq "test"
      seq_1_msg.raw_data.should eq "test".to_slice
      js.stream.get_msg(name, sequence: 2).not_nil!.message.data.should eq "another test"
      js.stream.get_msg(name, sequence: 3).not_nil!.message.data.should eq "yet another"

      js.stream.purge name, subject: first

      js.stream.info(name).not_nil!.state.messages.should eq 1
    ensure
      js.stream.delete stream if stream
    end
  end
end
