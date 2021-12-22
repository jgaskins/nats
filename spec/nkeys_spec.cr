require "./spec_helper"

require "../src/nkeys"

module NATS
  describe NKeys do
    # Values taken from the Ruby NKEYS library specs:
    # https://github.com/nats-io/nkeys.rb/blob/a942c0f607c410860845803d893b1e53673e8aa3/spec/nkeys_spec.rb#L19-L26
    seed = "SUAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU"
    nkeys = NKeys.new(seed)

    it "creates a public key" do
      # https://github.com/nats-io/nkeys.rb/blob/a942c0f607c410860845803d893b1e53673e8aa3/spec/nkeys_spec.rb#L94-L95
      Base32.encode(nkeys.keypair.public_key, pad: false).should eq "UCK5N7N66OBOINFXAYC2ACJQYFSOD4VYNU6APEJTAVFZB2SVHLKGEW7L"
    end

    it "signs a nonce" do
      nonce = "PXoWU7zWAMt75FY"

      signed_nonce = nkeys.keypair.sign(nonce)
      encoded_signed_nonce = Base64.strict_encode(signed_nonce)

      encoded_signed_nonce.should eq("ZaAiVDgB5CeYoXoQ7cBCmq+ZllzUnGUoDVb8C7PilWvCs8XKfUchAUhz2P4BYAF++Dg3w05CqyQFRDiGL6LrDw==")
    end
  end
end
