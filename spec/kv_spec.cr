require "./spec_helper"

require "../src/kv"
require "uuid"

private macro test(name)
  it {{name}} do
    name = UUID.random.to_s
    bucket = kv.create_bucket(name, history: 10)

    begin
      {{yield}}
    ensure
      kv.delete bucket
    end
  end
end

describe NATS::KV do
  nats = NATS::Client.new
  kv = nats.kv

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

  test "sets values" do
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

  test "creates a key or returns nil if the key already exists" do
    # Passes the first time because the key does not exist. Also, since this is
    # the first key being added to this store (see `test` macro above), the
    # revision is guaranteed to be `1`.
    bucket.create("key", "value").should eq 1

    # Fails the second time because the key already exists
    bucket.create("key", "value").should eq nil
  end

  describe "updating a key" do
    test "updates a key if the revision matches" do
      if revision = bucket.create "key", "value"
        bucket.update("key", "value2", revision).should eq 2
        bucket.get("key").not_nil!.value.should eq "value2".to_slice
      else
        raise "No revision returned from Bucket#create"
      end
    end

    test "does not update a key if the revision does not match" do
      if revision = bucket.create "key", "value"
        bucket.put "key", "value2"
        bucket.update("key", "value3", revision).should eq nil
        bucket.get("key").not_nil!.value.should eq "value2".to_slice
      else
        raise "No revision returned from Bucket#create"
      end
    end

    test "updates a key if another key is updated since the last time the given key was set" do
      bucket.put "key", "value"
      bucket.put "key2", "value2"

      # The revision is scoped to the bucket rather than to the key, so since
      # this is the third modification we've made to keys in this bucket, this
      # revision is #3.
      bucket.update("key", "value3", revision: 1).should eq 3

      bucket.get("key").not_nil!.value.should eq "value3".to_slice
    end
  end

  describe "getting a key" do
    test "a key that does not exist at all returns nil" do
      bucket.get("lol").should eq nil
    end

    test "a key that does exist will return a KV::Entry for that key" do
      bucket.put "key", "value"

      result = bucket.get("key").not_nil!

      result.value.should eq "value".to_slice
    end

    test "a key that has been deleted will return a KV::Entry whose `operation` is a `Delete`" do
      bucket.put "deleted", "value"
      bucket.delete "deleted"

      result = bucket.get("deleted").not_nil!

      result.value.should be_empty
      result.operation.delete?.should eq true
    end

    test "a key that has been deleted can return `nil` if you ignore deleted" do
      bucket.put "deleted", "value"
      bucket.delete "deleted"

      result = bucket.get("deleted", ignore_deletes: true)

      result.should be_nil
    end

    test "a key that is purged will show the purge item" do
      bucket.put "purged", "value"
      bucket.purge "purged"

      result = bucket.get("purged").not_nil!

      result.operation.purge?.should eq true
    end
  end

  describe "listing keys" do
    test "lists keys from Put operations" do
      bucket.put "a", "a"
      bucket.put "b", "b"

      bucket.keys.should contain "a"
      bucket.keys.should contain "b"
    end

    test "returns an empty set when there are no keys" do
      bucket.keys.should be_empty
    end

    test "ignores keys that are deleted or purged" do
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

    test "iterates over keys, ignoring history and deleted keys" do
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
  end

  describe "getting the history of a key" do
    test "it gets the full history of the key in chronological order" do
      key = "key"
      10.times do |i|
        bucket[key] = i.to_s
      end

      bucket.history(key).map(&.value).should eq Array.new(10, &.to_s.to_slice)
    end

    test "it returns an empty array if there is no history" do
      bucket.history("my-key").should be_empty
    end
  end
end
