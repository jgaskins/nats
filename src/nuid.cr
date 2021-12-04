module NATS
  class NUID
    DIGITS = "#{("0".."9").join}#{("A".."Z").join}#{("a".."z").join}".to_slice
    BASE = 62
    PREFIX_LENGTH = 12
    SEQUENCE_LENGTH = 10
    MAX_SEQUENCE = 839299365868340224_i64
    MIN_INCREMENT = 33_i64
    MAX_INCREMENT = 333_i64
    TOTAL_LENGTH = PREFIX_LENGTH + SEQUENCE_LENGTH

    class_getter global = new

    getter prefix : Bytes
    getter sequence : Int64
    getter increment : Int64
    @@lock = Mutex.new

    def initialize
      random = Random.new
      @sequence = random.rand(Int64).abs
      @increment = MIN_INCREMENT + random.rand(MAX_INCREMENT - MIN_INCREMENT)
      @prefix = Bytes.new(PREFIX_LENGTH)
      randomize_prefix
    end

    def self.next
      string = ""
      @@lock.synchronize { string = @@global.next }
      string
    end

    def next
      @sequence += increment
      if sequence >= MAX_SEQUENCE
        initialize
      end

      bytes = Bytes.new(TOTAL_LENGTH)
      prefix.copy_to bytes

      i = bytes.size
      l = sequence
      while i > PREFIX_LENGTH
        i -= 1
        bytes[i] = DIGITS[l % BASE]
        l //= BASE
      end

      String.new(bytes)
    end

    private def randomize_prefix
      bytes = Random::Secure.random_bytes(PREFIX_LENGTH)
      bytes.each_with_index do |byte, index|
        @prefix[index] = DIGITS[byte % BASE]
      end
    end
  end
end
