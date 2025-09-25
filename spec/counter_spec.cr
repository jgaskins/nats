require "./spec_helper"
require "../src/counter"

private macro create_counter(name, subjects, **counter_options)
  nats.counter.create(
    name: {{name}},
    subjects: {{subjects}},
    storage: :memory,
    {{counter_options.double_splat}}
  )
end

private macro test(name, counter_options = {} of String => String, **options)
  it({{name}}, {{options.double_splat}}) do
    write_subject = UUID.random.to_s
    stream_name = UUID.random.to_s
    counter = create_counter(stream_name, [write_subject], {{counter_options.double_splat}})

    begin
      {{yield}}
    ensure
      nats.jetstream.stream.delete stream_name
    end
  end
end

nats = NATS::Client.new
  .on_error { |ex| Log.for(NATS).error(exception: ex) }

describe NATS::Counter do
  test "increments a counter" do
    nats.counter.increment(write_subject).should eq 1
    nats.counter.increment(write_subject).should eq 2
  end

  test "gets the value of a counter" do
    nats.counter.get(write_subject).should eq 0

    nats.counter.increment write_subject
    nats.counter.get(write_subject).should eq 1

    nats.counter.increment write_subject, by: 99
    nats.counter.get(write_subject).should eq 100
  end

  test "increments a counter by a value other than 1" do
    delta = rand(2i64..Int64::MAX)

    nats.counter.increment(write_subject, by: delta).should eq delta
  end

  test "decrements a counter" do
    nats.counter.increment write_subject, by: 1000
    nats.counter.decrement(write_subject).should eq 999
    nats.counter.decrement(write_subject, by: 900).should eq 99
  end

  test "resets a counter" do
    nats.counter.increment write_subject, by: 100

    nats.counter.reset write_subject

    nats.counter.get(write_subject).should eq 0
  end

  test "sets a counter to a specific value" do
    nats.counter.set write_subject, 1234

    nats.counter.get(write_subject).should eq 1234
  end

  test "can increment without waiting for the server to acknowledge" do
    1_000.times do
      nats.counter.increment! write_subject
    end
    # Do something that *does* wait for the server to ack before we can assert
    # on the value. Otherwise, we get inconsistent results.
    nats.counter.increment write_subject

    nats.counter.get(write_subject).should eq 1_001
  end

  test "can decrement without waiting for the server to acknowledge" do
    1_000.times do
      nats.counter.decrement! write_subject
    end
    # Do something that *does* wait for the server to ack before we can assert
    # on the value. Otherwise, we get inconsistent results.
    nats.counter.decrement write_subject

    nats.counter.get(write_subject).should eq -1_001
  end
end
