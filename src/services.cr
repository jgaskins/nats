require "./nats"
require "./nuid"

module NATS
  # The [NATS Services API](https://docs.nats.io/using-nats/developer/services)
  # allows you to build microservices using NATS instead of HTTP. Since NATS has
  # the concept of request/reply messages through a decentralized message broker,
  # you don't need to deal with domain names, load-balancing, service meshes, or
  # other concepts that you might have to implement in HTTP-based microservices. NATS takes care of all of those for you.
  #
  # ```
  # inventory = nats.services.add "inventory",
  #   version: "0.1.0",
  #   description: "Manage store inventory"
  #
  # # Add the top-level subject namespace for the service
  # inventory.add_group "inventory" do |inv|
  #   # Add a namespace specifically for managing products
  #   inv.add_group "products" do |products|
  #     # Create a product. Receives a CreateProductRequest and returns a Product.
  #     # Example using the NATS CLI:
  #     #   nats req inventory.products.create '{"name":"Computer","description":"It is a computer","price_cents":200000}'
  #     products.add_endpoint "create" do |request|
  #       # Deserialize the request body and make a product out of it
  #       product = CreateProductRequest.from_json(request.data_string).product
  #       # Save the product to our KV store
  #       products_kv[product.id.to_s] = product.to_json
  #       # Reply with the serialized product
  #       nats.reply request, product.to_json
  #     end
  #
  #     # Return the product with the given id passed in the subject.
  #     # Example: inventory.products.get.0197e31f-3ffa-7fd7-8512-a331a20e5ae3
  #     # Equivalent to the following HTTP request:
  #     #   GET /inventory/products/0197e31f-3ffa-7fd7-8512-a331a20e5ae3
  #     products.add_endpoint "show", subject: "get.*" do |request|
  #       # Split the subject to get the id
  #       _, _, _, product_id = request.subject.split('.')
  #
  #       # Fetch the product from the `inventory-products` KV bucket
  #       nats.reply request, products_kv[product_id]
  #     end
  #   end
  # end
  # ```
  class Services
    @nats : Client

    NAME_PATTERN = /\A[\w\-]+\z/

    # :nodoc:
    def initialize(@nats)
    end

    # Register a service with the NATS server.
    #
    # ```
    # inventory = nats.services.add "inventory",
    #   version: "0.1.0",
    #   description: "Manage inventory"
    # ```
    def add(name : String, *, version : String, description : String, metadata : Hash(String, String) = {} of String => String)
      unless name =~ NAME_PATTERN
        raise ArgumentError.new("NATS service names may only contain A-Z, a-z, 0-9, dash, and underscore characters.")
      end
      started = Time.utc.to_rfc3339
      id = NUID.next
      service = Service.new(
        nats: @nats,
        id: id,
        name: name,
        version: version,
        description: description,
        metadata: metadata,
      )

      [
        "$SRV.PING",
        "$SRV.PING.#{name}",
        "$SRV.PING.#{name}.#{id}",
      ].each do |subject|
        service.subscriptions << @nats.subscribe subject do |msg|
          @nats.reply msg, {
            type:     "io.nats.micro.v1.ping_response",
            name:     name,
            id:       id,
            version:  version,
            metadata: metadata,
          }.to_json
        end
      end

      [
        "$SRV.STATS",
        "$SRV.STATS.#{name}",
        "$SRV.STATS.#{name}.#{id}",
      ].each do |subject|
        service.subscriptions << @nats.subscribe subject do |msg|
          @nats.reply msg, {
            type:      "io.nats.micro.v1.stats_response",
            name:      name,
            id:        id,
            version:   version,
            metadata:  metadata,
            endpoints: service.endpoint_stats,
            started:   started,
          }.to_json
        end
      end

      [
        "$SRV.INFO",
        "$SRV.INFO.#{name}",
        "$SRV.INFO.#{name}.#{id}",
      ].each do |subject|
        service.subscriptions << @nats.subscribe subject do |msg|
          @nats.reply msg, {
            type:        "io.nats.micro.v1.info_response",
            name:        name,
            id:          id,
            version:     version,
            description: description,
            metadata:    metadata,
            endpoints:   service.endpoint_info,
          }.to_json
        end
      end

      service
    end
  end

  # The `Service` represents a NATS service that was created by the `Services`
  # API. It exists to hang your endpoints on:
  #
  # ```
  # inventory = nats.services.add "inventory",
  #   version: "0.1.0",
  #   description: "Manage inventory"
  # inventory.add_endpoint "inventory.products.get", subject: "inventory.products.get.*" do |request|
  #   _, _, _, id
  #   nats.reply request, ProductQuery.new.find(id).to_json
  # end
  # ```
  class Service
    @nats : Client
    getter id : String
    getter name : String
    getter version : String
    getter description : String
    getter metadata : Hash(String, String)
    getter subscriptions : Array(Subscription)
    getter endpoints : Array(Endpoint) = [] of Endpoint
    @groups = [] of Group
    protected getter error_handler : ErrorHandler

    alias ErrorHandler = Exception, Message ->

    # :nodoc:
    def initialize(@nats, @id, @name, @version, @description, @metadata, @subscriptions = [] of Subscription)
      @error_handler = ErrorHandler.new do |error, message|
        @nats.reply message, "",
          headers: Headers{
            "Nats-Service-Error-Code" => "500",
            "Nats-Service-Error"      => error.message.try(&.gsub(/\s+/, ' ')) || "Internal error",
          }
      end
    end

    # ```
    # service.add_endpoint "endpoint-name", subject: "endpoint.subject" do |request|
    #   nats.reply request, "reply goes here"
    # end
    # ```
    def add_endpoint(
      name : String,
      *,
      subject : String = name,
      queue_group : String = "q",
      concurrency : Int = 1,
      &block : Message, Subscription ->
    ) : Endpoint
      unless name =~ Services::NAME_PATTERN
        raise ArgumentError.new("Endpoint names can only contain letter, number, dash, and underscore characters")
      end

      endpoint = uninitialized Endpoint
      subscription = @nats.subscribe subject, queue_group: queue_group, concurrency: concurrency do |msg, subscription|
        endpoint.request msg do
          block.call msg, subscription
        end
      end

      endpoint = Endpoint.new(
        nats: @nats,
        service: self,
        name: name,
        subject: subject,
        queue_group: queue_group,
        subscription: subscription,
      )
      endpoints << endpoint

      endpoint
    end

    # Add a `Group` and yield it to the block.
    #
    # ```
    # service.add_group "products" do |inventory|
    #   inventory.add_endpoint "create" do |request|
    #     nats.reply request, "reply goes here"
    #   end
    # end
    # ```
    def add_group(name : String, &)
      yield add_group(name)
    end

    # Add a `Group` of endpoints.
    def add_group(name : String)
      group = Group.new(@nats, self, name)
      @groups << group
      group
    end

    def error_reply(
      request : Message,
      body : String | Bytes = "",
      *,
      error : String,
      error_code : String | Int,
    ) : Nil
      @nats.reply request, body, headers: Headers{
        "Nats-Service-Error"      => error,
        "Nats-Service-Error-Code" => error_code.to_s,
      }
    end

    # Execute the given block for all exceptions raised in this service's
    # endpoints. This is useful for returning a common error response.
    #
    # ```
    # service.on_error do |exception, message|
    #   nats.reply message, "", headers: NATS::Headers{
    #     "Nats-Service-Error-Code" => "500",
    #     "Nats-Service-Error"      => "Unexpected error: #{exception.message}",
    #   }
    # end
    # ```
    def on_error(&@error_handler : ErrorHandler)
      self
    end

    # :nodoc:
    def endpoint_stats
      endpoints.map do |endpoint|
        {
          name:                    endpoint.name,
          subject:                 endpoint.subject,
          queue_group:             endpoint.queue_group,
          num_requests:            endpoint.total_requests,
          num_errors:              endpoint.total_errors,
          last_error:              endpoint.last_error,
          data:                    nil,
          processing_time:         endpoint.total_processing_time.total_nanoseconds.to_i64,
          average_processing_time: endpoint.average_processing_time.total_nanoseconds.to_i64,
        }
      end
    end

    # :nodoc:
    def endpoint_info
      endpoints.map do |endpoint|
        {
          type:        "io.nats.micro.v1.info_response",
          name:        endpoint.name,
          subject:     endpoint.subject,
          queue_group: endpoint.queue_group,
        }
      end
    end

    # The  `Group` represents a cohesive group of endpoints. Groups are useful
    # for applying a common prefix to subjects or setting a common queue group.
    class Group
      @nats : Client
      getter service : Service
      getter name : String

      # :nodoc:
      def initialize(@nats, @service, @name)
      end

      def add_endpoint(
        name : String,
        *,
        subject : String = name,
        queue_group : String = "q",
        concurrency : Int = 1,
        &block : Message, Subscription ->
      ) : Endpoint
        service.add_endpoint name,
          subject: "#{@name}.#{subject}",
          queue_group: queue_group,
          concurrency: concurrency,
          &block
      end

      def add_group(name : String, &)
        yield add_group(name)
      end

      def add_group(name : String)
        Group.new(@nats, @service, "#{@name}.#{name}")
      end
    end

    # The `Endpoint` is the request handler. You generally won't interact with
    # this class directly.
    class Endpoint
      @nats : Client
      getter service : Service
      getter name : String
      getter subject : String
      getter queue_group : String
      getter subscription : Subscription
      @total_requests : Atomic(Int64) = Atomic.new(0i64)
      @total_errors : Atomic(Int64) = Atomic.new(0i64)
      @total_processing_nanoseconds : Atomic(Int64) = Atomic.new(0i64)
      getter last_error : String?
      protected getter error_handler : ErrorHandler

      # :nodoc:
      def initialize(@nats, @service, @name, @subject, @queue_group, @subscription)
        @error_handler = @service.error_handler
      end

      def total_requests
        @total_requests.get
      end

      def total_errors
        @total_errors.get
      end

      def total_processing_time
        @total_processing_nanoseconds.get.nanoseconds
      end

      def average_processing_time
        requests = total_requests
        return 0.seconds if requests.zero?

        total_processing_time / total_requests
      end

      protected def request(message : Message, &)
        start = Time.monotonic
        begin
          yield
        rescue ex
          @total_errors.add 1
          @last_error = ex.message || ex.class.name
          @error_handler.call ex, message
          raise ex
        ensure
          @total_requests.add 1
          @total_processing_nanoseconds.add (Time.monotonic - start).total_nanoseconds.to_i64
        end
      end
    end
  end

  class Client
    getter services : Services { Services.new self }
  end
end
