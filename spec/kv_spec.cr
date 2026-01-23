require "./spec_helper"

require "../src/kv"
require "uuid"

private def test(name, bucket_options = {history: 10}, **options, &block : NATS::KV::Bucket, String ->)
  it name, **options do
    name = UUID.random.to_s
    bucket = kv.create_bucket(name, **bucket_options)

    begin
      block.call bucket, name
    ensure
      kv.delete bucket
    end
  end
end

private NATS_CLIENT = NATS::Client.new

private def nats
  NATS_CLIENT
end

def kv
  nats.kv
end

describe NATS::KV do
  if ENV["CLEAN_OUT_BUCKETS"]? == "true"
    nats.jetstream.stream.list.each do |stream|
      if stream.config.name =~ /\AKV_[[:xdigit:]]{8}-([[:xdigit:]]{4}-){3}[[:xdigit:]]{12}\z/
        nats.jetstream.stream.delete stream
      end
    end
  end

  it "creates and deletes buckets (streams prefixed with `KV_`)" do
    name = UUID.random.to_s
    bucket = kv.create_bucket(name)
    bucket.name.should eq name
    kv.get_bucket(name).not_nil!.name.should eq name
    kv.get_bucket(name).should be_a NATS::KV::Bucket

    kv.delete_bucket name
    kv.get_bucket(name).should eq nil
  end

  test "lists buckets" do |bucket, name|
    bucket["one"] = "1"
    bucket["two"] = "2"

    buckets = kv.list_buckets(pattern: name)

    buckets.total.should eq 1
    buckets.limit.should eq 256
    buckets.offset.should eq 0
    buckets.first.name.should eq name
    buckets.first.values.should eq 2
  end

  test "sets values" do |bucket, name|
    bucket.put "key", "value"

    if result = bucket.get("key")
      result.revision.should eq 1
      result.key.should eq "key"
      result.value.should eq "value".to_slice
      result.operation.put?.should eq true
      result.bucket.should eq name
    else
      raise "Could not fetch key from bucket, result was `nil`"
    end
  end

  test "sets values with a TTL", bucket_options: {allow_msg_ttl: true} do |bucket, name|
    # We create a lot of values with TTLs to mitigate false positive & negatives
    # due to sending/checking them at just the right time. I hate flaky tests.
    concurrency = 10_000
    channel = Channel(Exception?).new(concurrency)
    concurrency.times do |i|
      spawn do
        key = "key-#{i}"
        bucket.put key, "value", ttl: 1.second
        bucket.get(key).should_not eq nil
        sleep 2.second # per-message TTLs apparently have 1-second precision 😞
        bucket.get(key).should eq nil
      rescue ex
        error = ex
      ensure
        channel.send error
      end
    end

    concurrency.times do
      if error = channel.receive
        raise error
      end
    end
  end

  test "gets a specific revision of a key" do |bucket, name|
    bucket.put "key", "value1"
    bucket.put "key", "value2"
    revision = bucket.put "another-key", "another value"
    bucket.put "key", "value3"
    bucket.put "key", "value4"

    bucket.get!("key", revision: revision).value.should eq "value3".to_slice
  end

  test "creates a key or returns nil if the key already exists" do |bucket, name|
    # Passes the first time because the key does not exist. Also, since this is
    # the first key being added to this store (see `test` macro above), the
    # revision is guaranteed to be `1`.
    bucket.create("key", "value").should eq 1

    # Fails the second time because the key already exists
    bucket.create("key", "value").should eq nil
  end

  describe "updating a key" do
    test "updates a key if the revision matches" do |bucket, name|
      if revision = bucket.create "key", "value"
        bucket.update("key", "value2", revision).should eq 2
        bucket.get!("key").value.should eq "value2".to_slice
      else
        raise "No revision returned from Bucket#create"
      end
    end

    test "does not update a key if the revision does not match" do |bucket, name|
      if revision = bucket.create "key", "value"
        bucket.put "key", "value2"
        bucket.update("key", "value3", revision).should eq nil
        bucket["key"]?.should eq "value2".to_slice
      else
        raise "No revision returned from Bucket#create"
      end
    end

    test "does not update a key if history is exceeded and discard_new_per_key is set", bucket_options: {history: 2, discard_new_per_key: true} do |bucket, name|
      bucket.put "a", "1"
      bucket.put "a", "2"

      expect_raises NATS::KV::Error, "maximum messages per subject exceeded" do
        bucket.put("a", "3")
      end
    end

    test "updates a key if another key is updated since the last time the given key was set" do |bucket, name|
      bucket.put "key", "value"
      bucket.put "key2", "value2"

      # The revision is scoped to the bucket rather than to the key, so since
      # this is the third modification we've made to keys in this bucket, this
      # revision is #3.
      bucket.update("key", "value3", revision: 1).should eq 3

      bucket["key"]?.should eq "value3".to_slice
    end
  end

  describe "getting a key" do
    test "a key that does not exist at all returns nil" do |bucket, name|
      bucket.get("lol").should eq nil
    end

    test "a key that does exist will return a KV::Entry for that key" do |bucket, name|
      bucket.put "key", "value"

      result = bucket.get("key").not_nil!

      result.value.should eq "value".to_slice
    end

    test "a key that has been deleted will return a KV::Entry whose `operation` is a `Delete`" do |bucket, name|
      bucket.put "deleted", "value"
      bucket.delete "deleted"

      result = bucket.get("deleted").not_nil!

      result.value.should be_empty
      result.operation.delete?.should eq true
    end

    test "a key that has been deleted can return `nil` if you ignore deleted" do |bucket, name|
      bucket.put "deleted", "value"
      bucket.delete "deleted"

      result = bucket.get("deleted", ignore_deletes: true)

      result.should be_nil
    end

    test "a key that is purged will show the purge item" do |bucket, name|
      bucket.put "purged", "value"
      bucket.purge "purged"

      result = bucket.get("purged").not_nil!

      result.operation.purge?.should eq true
    end
  end

  describe "listing keys" do
    test "lists keys from Put operations" do |bucket, name|
      bucket.put "a", "a"
      bucket.put "b", "b"

      bucket.keys.should contain "a"
      bucket.keys.should contain "b"
    end

    test "returns an empty set when there are no keys" do |bucket, name|
      bucket.keys.should be_empty
    end

    test "ignores keys that are deleted or purged" do |bucket, name|
      bucket.put "a", "a"
      bucket.put "b", "b"
      bucket.put "deleted", "lol"
      bucket.put "purged", "omg"

      bucket.delete "deleted"
      bucket.purge "purged"

      bucket.keys.should contain "a"
      bucket.keys.should contain "b"
      bucket.keys.should_not contain "deleted"
      bucket.keys.should_not contain "purged"
    end

    test "iterates over keys, ignoring history and deleted keys" do |bucket, name|
      bucket["deleted"] = "delete me now plz"
      bucket.delete "deleted"
      bucket["a"] = "a"
      bucket["b"] = "b"
      bucket["purged"] = "purge me"
      bucket.purge "purged"

      bucket.each_key do |key|
        %w[a b].should contain key
      end
    end

    test "iterates over keys with a specific pattern" do |bucket|
      bucket["included.1"] = "1"
      bucket["included.2"] = "2"
      bucket["excluded.1"] = "nope"

      count = 0
      bucket.each_key pattern: "included.*" do |key|
        count += 1
        key.should start_with "included."
      end
      count.should eq 2

      count = 0
      bucket.each_key pattern: "*.1" do |key|
        count += 1
        key.should end_with ".1"
      end
      count.should eq 2
    end
  end

  describe "getting the history of a key" do
    test "it gets the full history of the key in chronological order" do |bucket, name|
      key = "key"
      10.times do |i|
        bucket[key] = i.to_s
      end

      bucket.history(key).map(&.value).should eq Array.new(10, &.to_s.to_slice)
    end

    test "it returns an empty array if there is no history" do |bucket, name|
      bucket.history("my-key").should be_empty
    end
  end
end
