require "./entity"
require "./consumer"

module NATS::JetStream
  struct ConsumerListResponse < Entity
    include Enumerable(Consumer)

    getter total : Int64
    getter offset : Int64
    getter limit : Int64
    getter consumers : Array(Consumer) { [] of Consumer }
    @[JSON::Field(ignore: true)]
    protected property! js : JetStream::Client
    @[JSON::Field(ignore: true)]
    protected property! stream_name : String

    def self.new(js : JetStream::Client, json : JSON::PullParser, stream_name : String)
      response = new json
      response.js = js
      response.stream_name = stream_name
      response
    end

    # Yields every consumer for the stream, requesting more pages from the
    # server as needed.
    def each(&block : Consumer ->) : Nil
      consumers.each { |c| yield c }
      new_offset = offset + consumers.size
      total = self.total
      while total > new_offset
        response = js
          .consumer
          .list(stream_name, offset: new_offset)

        break if response.consumers.empty?
        response
          .consumers
          .each { |c| yield c }
        total = response.total
        new_offset = response.offset + response.consumers.size
      end
    end
  end

  deprecate_api_v1 ConsumerListResponse
end
