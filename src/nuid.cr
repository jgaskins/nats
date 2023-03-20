module NATS
  class NUID
    DIGITS          = "#{("0".."9").join}#{("A".."Z").join}#{("a".."z").join}".to_slice
    BASE            =                     62
    PREFIX_LENGTH   =                     12
    SEQUENCE_LENGTH =                     10
    MAX_SEQUENCE    = 839299365868340224_i64
    MIN_INCREMENT   =                 33_i64
    MAX_INCREMENT   =                333_i64
    TOTAL_LENGTH    = PREFIX_LENGTH + SEQUENCE_LENGTH

    class_getter global : self { new }

    getter prefix : Bytes { Bytes.new(PREFIX_LENGTH) }
    getter sequence : Atomic(Int64)
    getter increment : Int64

    def initialize(random = Random.new)
      @sequence = Atomic.new(random.rand(MAX_SEQUENCE).abs)
      @increment = random.rand(MIN_INCREMENT..MAX_INCREMENT)
      randomize_prefix
    end

    def self.next
       global.next
    end

    def next
      seq = sequence.add increment
      if seq >= MAX_SEQUENCE - increment
        initialize
      end

      stringify seq
    end

    def to_s
      stringify sequence.get
    end

    private def stringify(seq : Int64)
      String.new 22 do |buffer|
        prefix.copy_to buffer, prefix.bytesize

        SEQUENCE_LENGTH.times do |i|
          buffer[21 - i] = DIGITS[seq % BASE]
          seq //= BASE
        end

        {22, 22}
      end
    end

    private def randomize_prefix
      bytes = Random::Secure.random_bytes(PREFIX_LENGTH)
      bytes.each_with_index do |byte, index|
        prefix[index] = DIGITS[byte % BASE]
      end
    end
  end
end
