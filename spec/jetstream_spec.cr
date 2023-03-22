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

private macro create_consumer(stream, deliver_subject = UUID.random.to_s)
  nats.jetstream.consumer.create(
    stream_name: stream.config.name,
    deliver_group: UUID.random.to_s,
    deliver_subject: {{deliver_subject}},
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

  it "paginates streams" do
    a_bunch_of_streams = Array.new(1000) do |id|
      js.stream.create(
        name: "test-#{id}",
        storage: :memory,
        subjects: ["test.jetstream.pagination.#{id}"],
      )
    end

    begin
      streams = nats.jetstream.stream.list.to_a
      a_bunch_of_streams.each do |stream|
        streams.map(&.config.name).should contain stream.config.name
      end
    ensure
      a_bunch_of_streams.each do |stream|
        js.stream.delete stream
      end
    end
  end

  it "creates and deletes streams" do
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
      ack_policy: :none,
    )

    begin
      js.publish write_subject, "hello"
      channel = Channel(NATS::JetStream::Message).new

      msg_subject = nil
      msg_body = nil
      nats.jetstream.subscribe consumer do |msg|
        channel.send msg
      end

      select
      when msg = channel.receive
        msg.subject.should eq write_subject
        msg.body.should eq "hello".to_slice
      when timeout(2.seconds)
        raise "Did not receive message within 2 seconds"
      end
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

    it "can wait for double-ack for a list of messages" do
      successful = [] of NATS::JetStream::Message
      write_subject = UUID.random.to_s
      channel = Channel(NATS::JetStream::Message).new
      stream = create_stream([write_subject])
      consumer = create_consumer(stream)
      count = 10
      count.times { |i| nats.publish write_subject, i.to_s }

      begin
        nats.jetstream.subscribe consumer do |msg|
          channel.send msg
        end

        count.times { successful << channel.receive }
        nats.jetstream.ack_sync(successful).should eq count
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

    it "can ack a pull message and receive the next in the same request" do
      write_subject = UUID.random.to_s
      stream = create_stream([write_subject])
      consumer = create_consumer(stream, deliver_subject: nil)
      3.times do |i|
        nats.jetstream.publish write_subject, (i + 1).to_s
      end

      begin
        count = 0
        pull = nats.jetstream.pull_subscribe(consumer)
        msg = pull.fetch(1).first
        while msg = pull.ack_next(msg, timeout: 100.milliseconds)
          count += 1
        end

        count.should eq 2
      ensure
        nats.jetstream.stream.delete stream
      end
    end
  end

  it "gets idle heartbeats" do
    write_subject = UUID.random.to_s
    stream = create_stream([write_subject])
    consumer_name = UUID.random.to_s
    deliver_group = UUID.random.to_s
    consumer = nats.jetstream.consumer.create(
      stream_name: stream.config.name,
      deliver_subject: consumer_name,
      deliver_group: deliver_group,
      idle_heartbeat: 150.milliseconds,
    )
    10.times { nats.publish write_subject, "hi" }
    nats.jetstream.subscribe consumer do |msg|
      nats.jetstream.ack_sync msg
    end

    sleep 250.milliseconds

    nats.jetstream.stream.delete stream
  end

  it "reads from durable pull consumers" do
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
        msg.body.should eq "0".to_slice

        nats.jetstream.ack msg
      else
        raise "Did not receive a message within 2 seconds"
      end

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

  it "reads from ephemeral pull consumers" do
    write_subject = UUID.random.to_s
    consumer_name = UUID.random.to_s
    stream = create_stream([write_subject])
    consumer = nats.jetstream.consumer.create(
      stream_name: stream.config.name,
      max_deliver: -1
    )
    pull = nats.jetstream.pull_subscribe(consumer)

    begin
      3.times { |i| nats.publish write_subject, i.to_s }

      if msg = pull.fetch(timeout: 5000.milliseconds)
        msg.subject.should eq write_subject
        msg.body.should eq "0".to_slice
      else
        raise "Did not receive a message within 500ms"
      end

      msgs = pull.fetch(3, timeout: 500.milliseconds)
      msgs.map(&.body).should eq [
        "1".to_slice,
        "2".to_slice,
      ]
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

      js.stream.get_msg(name, sequence: 1).not_nil!.message.data.should eq "test".to_slice
      js.stream.get_msg(name, sequence: 2).not_nil!.message.data.should eq "another test".to_slice
      js.stream.get_msg(name, sequence: 3).not_nil!.message.data.should eq "yet another".to_slice

      js.stream.purge name, subject: first

      js.stream.info(name).not_nil!.state.messages.should eq 1
    ensure
      js.stream.delete stream if stream
    end
  end
end
