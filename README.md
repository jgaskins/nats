# NATS

NATS is a message broker for distributed systems.

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     nats:
       github: jgaskins/nats
   ```

2. Run `shards install`

## Usage

You can use NATS in a publish/subscribe or request/reply paradigm.

### Publish/Subscribe

For publish/subscribe, let's consider the following class to be shared, representing an event that will be published by one service and picked up by another:

```crystal
require "uuid"
require "json"
require "uuid/json"

struct UserRegisteredEvent
  include JSON::Serializable

  getter id : UUID
  getter email : String
  getter name : String

  def initialize(@id, @email, @name)
  end
end
```

In one service, we can subscribe to a subject that will be sent all of the events pertaining to a user registering:

```crystal
require "nats"

nats = NATS::Client.new(URI.parse(ENV["NATS_URL"]))

# Subscribe to all messages on "customers.registration" with an optional queue
# group. A message will only be delivered to a single client in a given queue
# group.
nats.subscribe "customers.registration", queue_group: "cart-service" do |msg|
  new_user = UserRegisteredEvent.from_json(String.new msg.payload)

  # This message represents that a new customer has registered, presumably sent
  # by our identity/authentication/user service. We create a record for this
  # customer in our own database so we don't always need to request the info
  # from that service.
  UserQuery.new.create_from_message(new_user)
end

# Accept wildcard messages. This would match:
# - orders.commercial.fulfilled
# - orders.individual.fulfilled
nats.subscribe "orders.*.shipped", queue_group: "cart-service" do |msg|
  # ...
end

# Since the subscribe blocks above do not block execution, we need to keep the
# main fiber from exiting. In a real-world app, you might trap a TERM/INT signal
# to allow the app to close the connection gracefully.
sleep
```

And then to publish on those topics:

```crystal
require "nats"

nats = NATS::Client.new(URI.parse(ENV["NATS_URL"]))

# We can publish a message with a given subject. In this example, we'll
# publish a message saying Jolene has registered.
nats.publish "customers.registration", UserRegisteredEvent.new(
  id: UUID.random,
  name: "Jolene",
  email: "jolene@gonnatakeyourman.com",
).to_json

nats.close
```

### Request/Reply

Let's consider an orders service that we may want to send requests to.

```crystal
require "uuid"
require "json"
require "uuid/json"
require "db"

module Orders
  struct Get
    include JSON::Serializable

    getter id : UUID

    def initialize(@id)
    end
  end
end

struct Order
  include DB::Serializable
  include JSON::Serializable

  getter id : UUID
  getter address : String
  getter city : String
  getter state : String
  getter postal_code : String
end
```

#### Define the request handler

```crystal
require "nats"

nats = NATS::Client.new(URI.parse(ENV["NATS_URL"]))

# Subscribe to the subject that the request will be sent to
nats.subscribe "orders.get", do |msg|
  request = Orders::Get.from_json(String.new msg.payload)

  order = OrderQuery.new.with_id(request.id)

  nats.reply msg, order.to_json
end
```

#### Sending the request

```crystal
require "nats"

nats = NATS::Client.new(URI.parse(ENV["NATS_URL"]))

# Send the request to the subject it is expected to be received on:
order = nats.request "orders.get",
  message: Orders::Get.new(order_id).to_json,
  timeout: 5.seconds # A timeout must be specified

pp order
```

## Development

TODO: Write development instructions here

## Contributing

1. Fork it (<https://github.com/jgaskins/nats/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Jamie Gaskins](https://github.com/jgaskins) - creator and maintainer
