require "./spec_helper"
require "../src/nuid"

module NATS
  describe NUID do
    # Adapted from https://github.com/nats-io/nuid/blob/94bac9a67a7388c50abc3f3d127cb9af09ec110b/nuid_test.go#L27-L37
    it "is always initialized" { NUID.global.should_not be_nil }
    it "always has a prefix" { NUID.global.prefix.should_not be_nil }
    it "has a nonzero prefix" { NUID.global.sequence.should_not eq 0 }

    it "can get the next NUID, which changes the global NUID" do
      nuid = NUID.global.to_s
      NUID.next.should be_a String
      NUID.global.should_not eq nuid
    end
  end
end
