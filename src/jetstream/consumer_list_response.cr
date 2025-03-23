require "./entity"
require "./consumer"

module NATS::JetStream
  struct ConsumerListResponse < Entity
    include Enumerable(Consumer)

    getter total : Int64
    getter offset : Int64
    getter limit : Int64
    getter consumers : Array(Consumer)

    def each(&)
      consumers.each { |c| yield c }
    end
  end

  deprecate_api_v1 ConsumerListResponse
end
