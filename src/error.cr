module NATS
  # Generic error
  class Error < ::Exception
  end

  # Raised when the server sends a protocol event this client doesn't recognize.
  class UnknownCommand < Error
  end
end
