require "string_pool"

require "./log"
require "./error"
require "./headers"
require "./message"
require "./server_info"

module NATS
  # :nodoc:
  module Protocol
    abstract struct Event
      getter type : Type

      def initialize(@type)
      end

      enum Type
        Message
        OK
        Ping
        Pong
        Info
        Error
        Unknown
      end
    end

    struct MessageEvent < Event
      getter! message : Message
      getter! sid : String

      def initialize(@message, @sid)
        super :message
      end
    end

    struct OKEvent < Event
      def initialize
        super :ok
      end
    end

    struct PingEvent < Event
      def initialize
        super :ping
      end
    end

    struct PongEvent < Event
      def initialize
        super :pong
      end
    end

    struct InfoEvent < Event
      getter server_info : ServerInfo

      def initialize(@server_info)
        super :info
      end
    end

    struct ErrorEvent < Event
      getter error : Exception

      def initialize(@error)
        super :error
      end
    end

    struct UnknownEvent < Event
      getter error : Exception

      def initialize(@error)
        super :unknown
      end
    end
  end

  # :nodoc:
  struct EventParser
    @io : IO

    STRING_POOL       = StringPool.new
    STRING_POOL_MUTEX = Mutex.new

    private LINE_BUFFER_SIZE = 4096
    private HEADER_PREAMBLE  = "NATS/1.0"

    # `#call` consumes the first byte of the line to tell messages apart from
    # everything else, so these are the *remainders* of each control line.
    private PING_REST = "ING".to_slice
    private PONG_REST = "ONG".to_slice
    private OK_REST   = "OK".to_slice
    private INFO_REST = "NFO ".to_slice
    private ERR_REST  = "ERR ".to_slice

    def initialize(@io)
    end

    def call : Protocol::Event
      buffer = uninitialized UInt8[LINE_BUFFER_SIZE]
      bytes = buffer.to_slice

      case first = @io.read_byte
      when nil
        raise IO::EOFError.new
      when 'M'.ord # MSG
        @io.skip 2 # "SG"
        message, sid = parse_message bytes, headers: false
        Protocol::MessageEvent.new(message: message, sid: sid)
      when 'H'.ord # HMSG
        @io.skip 3 # "MSG"
        message, sid = parse_message bytes, headers: true
        Protocol::MessageEvent.new(message: message, sid: sid)
      else
        parse_control first, read_line(bytes)
      end
    end

    # Every event that isn't a message occupies a single line: `PING`, `PONG`,
    # `+OK`, `INFO {…}`, or `-ERR …`. *first* is the byte `#call` consumed to
    # work out which one it was, *rest* is the remainder of that line.
    private def parse_control(first : UInt8, rest : Bytes) : Protocol::Event
      case first
      when 'P'.ord
        case rest
        when PING_REST then return Protocol::PingEvent.new
        when PONG_REST then return Protocol::PongEvent.new
        end
      when '+'.ord
        return Protocol::OKEvent.new if rest == OK_REST
      when 'I'.ord
        if starts_with? rest, INFO_REST
          json = String.new(rest[INFO_REST.size..])
          return Protocol::InfoEvent.new(ServerInfo.from_json(json))
        end
      when '-'.ord
        if starts_with? rest, ERR_REST
          return Protocol::ErrorEvent.new(Error.new("-#{String.new(rest)}"))
        end
      end

      line = "#{first.unsafe_chr}#{String.new(rest)}"
      Protocol::UnknownEvent.new(UnknownCommand.new(line))
    end

    private def starts_with?(bytes : Bytes, prefix : Bytes) : Bool
      bytes.size >= prefix.size && bytes[0, prefix.size] == prefix
    end

    # An `MSG` event from the server looks like this (brackets imply optional):
    #
    #     MSG my-subject my-sid [my-reply-to] payload_size
    #     My payload goes here
    #
    # An `HMSG` event carries a block of headers ahead of the payload:
    #
    #     HMSG my-subject my-sid [my-reply-to] header_size total_size
    #     NATS/1.0
    #     My-Key: My-Value
    #
    #     My payload goes here
    #
    # The total size includes the headers, so the payload is
    # `total_size - header_size` bytes long.
    private def parse_message(buffer : Bytes, headers has_headers) : {Message, String}
      line = read_line buffer

      subject_bytes, cursor = next_token line, 0
      sid_bytes, cursor = next_token line, cursor
      third, cursor = next_token line, cursor
      fourth, cursor = next_token line, cursor

      if has_headers
        fifth, cursor = next_token line, cursor
        if fifth.empty? # No reply-to: HEADER_SIZE TOTAL_SIZE
          reply_to_bytes = fifth
          header_size = parse_int third
          total_size = parse_int fourth
        else # REPLY_TO HEADER_SIZE TOTAL_SIZE
          reply_to_bytes = third
          header_size = parse_int fourth
          total_size = parse_int fifth
        end
        body_size = total_size - header_size
      else
        if fourth.empty? # No reply-to: PAYLOAD_SIZE
          reply_to_bytes = fourth
          body_size = parse_int third
        else # REPLY_TO PAYLOAD_SIZE
          reply_to_bytes = third
          body_size = parse_int fourth
        end
      end

      if subject_bytes.empty? || sid_bytes.empty? || body_size < 0
        raise Error.new("Invalid message declaration: #{String.new(line).inspect}")
      end

      # These have to be copied out of the buffer before we read the headers,
      # since reading the header block overwrites it.
      subject = String.new(subject_bytes)
      sid = String.new(sid_bytes)
      reply_to = String.new(reply_to_bytes) unless reply_to_bytes.empty?

      if has_headers
        headers = read_headers buffer
      end

      body = @io.read_string(body_size)
      expect_crlf

      msg = Message.new subject, body,
        reply_to: reply_to,
        headers: headers

      {msg, sid}
    end

    # Reads the header block of an `HMSG`, which is a `NATS/1.0` preamble
    # followed by `Key: Value` lines and terminated by a blank line. Anything
    # trailing the preamble is the request status, which we expose as the
    # `Status` header.
    private def read_headers(buffer : Bytes) : Headers
      preamble = read_line buffer
      unless preamble.size >= HEADER_PREAMBLE.bytesize && preamble[0, HEADER_PREAMBLE.bytesize] == HEADER_PREAMBLE.to_slice
        raise Error.new("Invalid header declaration: #{String.new(preamble).inspect}")
      end

      headers = Headers.new
      if preamble.size > HEADER_PREAMBLE.bytesize + 1
        headers["Status"] = String.new(preamble[HEADER_PREAMBLE.bytesize + 1..])
      end

      until (line = read_line buffer).empty?
        unless separator = line.index ':'.ord.to_u8!
          raise Error.new("Invalid header line: #{String.new(line).inspect}")
        end

        value_start = separator + 1
        while value_start < line.size && line[value_start] == ' '.ord
          value_start += 1
        end

        headers.add intern(line[0, separator]), String.new(line[value_start..])
      end

      LOG.trace { "Headers: #{headers.inspect}" }

      headers
    end

    # Returns the next space-delimited token in *line* starting at *cursor*,
    # along with the cursor position just past it. An empty slice means the line
    # held no more tokens.
    private def next_token(line : Bytes, cursor : Int32) : {Bytes, Int32}
      while cursor < line.size && line[cursor] == ' '.ord
        cursor += 1
      end

      start = cursor
      while cursor < line.size && line[cursor] != ' '.ord
        cursor += 1
      end

      {line[start, cursor - start], cursor}
    end

    private def parse_int(bytes : Bytes) : Int32
      return -1 if bytes.empty?

      int = 0
      bytes.each do |byte|
        return -1 unless '0'.ord <= byte <= '9'.ord
        int = (int * 10) + (byte - '0'.ord)
      end

      int
    end

    # Reads up to the next CRLF, returning the line without its terminator. The
    # returned slice points into *buffer* (or into a larger heap-allocated
    # buffer if the line didn't fit) and is only valid until the next read.
    private def read_line(buffer : Bytes) : Bytes
      size = 0

      loop do
        if peek = @io.peek
          raise IO::EOFError.new if peek.empty?

          if index = peek.index('\r'.ord.to_u8)
            buffer = append buffer, size, peek[0, index]
            size += index
            @io.skip index + 1 # the line plus the CR
            expect_lf
            return buffer[0, size]
          else
            buffer = append buffer, size, peek
            size += peek.size
            @io.skip peek.size
          end
        else # The IO doesn't support peeking, so we go byte by byte
          byte = @io.read_byte || raise IO::EOFError.new
          if byte == '\r'.ord
            expect_lf
            return buffer[0, size]
          end

          buffer = grow buffer, size + 1
          buffer[size] = byte
          size += 1
        end
      end
    end

    private def append(buffer : Bytes, size : Int32, bytes : Bytes) : Bytes
      return buffer if bytes.empty?

      buffer = grow buffer, size + bytes.size
      bytes.copy_to buffer[size, bytes.size]
      buffer
    end

    private def grow(buffer : Bytes, needed : Int32) : Bytes
      return buffer if needed <= buffer.size

      larger = Bytes.new(Math.pw2ceil(needed))
      buffer.copy_to larger[0, buffer.size]
      larger
    end

    private def expect_crlf : Nil
      unless @io.read_byte == '\r'.ord
        raise Error.new("Expected CR")
      end
      expect_lf
    end

    private def expect_lf : Nil
      unless @io.read_byte == '\n'.ord
        raise Error.new("Expected LF")
      end
    end

    # Header names come from a small, fixed vocabulary, so interning them avoids
    # allocating the same handful of strings over and over. Note that the pool
    # never evicts, so only low-cardinality values belong in here — subjects,
    # SIDs, and reply-to subjects are all unbounded and must not be interned.
    private def intern(bytes : Bytes) : String
      STRING_POOL_MUTEX.synchronize { STRING_POOL.get bytes }
    end
  end
end
