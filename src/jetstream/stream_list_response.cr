require "json"

require "./entity"
require "./stream"
require "./client"

module NATS::JetStream
  struct StreamListResponse < Entity
    include Enumerable(Stream)

    getter total : Int64
    getter offset : Int64
    getter limit : Int64
    getter streams : Array(Stream) { [] of Stream }
    @[JSON::Field(ignore: true)]
    protected property! js : JetStream::Client
    @[JSON::Field(ignore: true)]
    protected property subject : String?

    def self.new(js : JetStream::Client, json : JSON::PullParser, subject : String? = nil)
      response = new json
      response.js = js
      response.subject = subject
      response
    end

    def each(&block : Stream ->) : Nil
      streams.each { |s| yield s }
      new_offset = offset + limit
      total = self.total
      while total > new_offset
        response = js
          .stream
          .list(subject: subject, offset: new_offset, limit: limit)

        response
          .streams
          .each { |s| yield s }
        total = response.total
        new_offset = response.offset + response.limit
      end
    end
  end

  deprecate_api_v1 StreamListResponse
end
