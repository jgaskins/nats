require "./spec_helper"
require "../src/objects"
require "uuid"

private macro test(name, **options)
  it({{name}}, {{options.double_splat}}) do
    name = UUID.random.to_s
    bucket = obj.create_bucket(name)
    begin
      {{yield}}
    ensure
      obj.delete_bucket name
    end
  end
end

module NATS
  nats = Client.new
  obj = nats.objects

  describe Objects do
    it "creates and deletes object stores" do
      name = UUID.random.to_s

      bucket = obj.create_bucket(name, storage: :memory)

      bucket.should be_a Objects::Bucket
    ensure
      if name && bucket
        obj.delete_bucket(name)
      end
    end

    test "creates an object from a string" do
      bucket.put "key", "value", headers: NATS::Headers{"foo" => "bar"}

      info = bucket.get_info!("key")

      info.digest.should eq "SHA-256=#{sha256("value")}"
      info.headers["foo"].should eq "bar"
      info.size.should eq 5
      info.chunks.should eq 1 # We used the default chunk size of 128KB, which easily holds 5 bytes
      bucket.get!("key").read_string(info.size).should eq "value"
    end

    test "creates an object from a file" do
      file = File.tempfile { |f| f << "value" }

      File.open(file.path) { |f| bucket.put "key", f }

      info = bucket.get_info!("key")
      expected_digest = "SHA-256=#{sha256("value")}"
      info.digest.should eq expected_digest
      bucket.get!("key").read_string(info.size).should eq "value"
    ensure
      file.delete if file
    end

    test "gets large objects" do
      io = IO::Memory.new(Random::Secure.random_bytes(10_000_000))

      bucket.put "key", io
      info = bucket.get_info!("key")
      data = bucket.get!("key").gets_to_end

      data.should eq io.to_s
    end

    test "gets info for a key that has dots in it" do
      bucket.put "key.value", "value"

      obj.get_info(bucket.name, "key.value").should_not be_nil
    end

    test "deletes an object" do
      bucket.put "key", "value"
      object = bucket.get_info("key")

      object.should_not eq nil
      bucket.delete("key")
      # A deleted object not being returned from `get_info` is implicitly tested
      # here. There is still an object in the metadata subject after deletion.
      bucket.get_info("key").should eq nil
    end

    test "gets keys for a bucket" do
      bucket.put "key", "value", headers: Headers{"foo" => "bar"}

      obj.keys(bucket.name).should contain "key"
    end
  end
end

private def sha256(string : String)
  Base64.urlsafe_encode(Digest::SHA256.new.tap { |sha| sha << string }.final)
end
