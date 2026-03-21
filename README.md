# fila-ruby

Ruby client SDK for the [Fila](https://github.com/faisca/fila) message broker.

## Installation

```bash
gem install fila-client
```

Or add to your Gemfile:

```ruby
gem "fila-client"
```

## Usage

```ruby
require "fila"

client = Fila::Client.new("localhost:5555")

# Enqueue a message.
msg_id = client.enqueue(
  queue: "my-queue",
  headers: { "tenant" => "acme" },
  payload: "hello world"
)
puts "Enqueued: #{msg_id}"

# Consume messages (block form).
client.consume(queue: "my-queue") do |msg|
  puts "Received: #{msg.id} (attempt #{msg.attempt_count})"

  begin
    # Process the message...
    client.ack(queue: "my-queue", msg_id: msg.id)
  rescue => e
    client.nack(queue: "my-queue", msg_id: msg.id, error: e.message)
  end
end

client.close
```

### TLS

```ruby
require "fila"

# Server-side TLS (verify server certificate).
client = Fila::Client.new("localhost:5555",
  ca_cert: File.read("ca.pem")
)
```

### mTLS (mutual TLS)

```ruby
require "fila"

# Mutual TLS — both sides present certificates.
client = Fila::Client.new("localhost:5555",
  ca_cert: File.read("ca.pem"),
  client_cert: File.read("client.pem"),
  client_key: File.read("client-key.pem")
)
```

### API Key Authentication

```ruby
require "fila"

# API key sent as Bearer token on every request.
client = Fila::Client.new("localhost:5555",
  api_key: "fila_your_api_key_here"
)
```

### mTLS + API Key

```ruby
require "fila"

# Full security: mTLS transport + API key authentication.
client = Fila::Client.new("localhost:5555",
  ca_cert: File.read("ca.pem"),
  client_cert: File.read("client.pem"),
  client_key: File.read("client-key.pem"),
  api_key: "fila_your_api_key_here"
)
```

## API

### `Fila::Client.new(addr, ca_cert: nil, client_cert: nil, client_key: nil, api_key: nil)`

Connect to a Fila broker at the given address (e.g., `"localhost:5555"`).

| Parameter | Type | Description |
|---|---|---|
| `addr` | `String` | Broker address in `"host:port"` format |
| `ca_cert:` | `String` or `nil` | PEM-encoded CA certificate for TLS |
| `client_cert:` | `String` or `nil` | PEM-encoded client certificate for mTLS |
| `client_key:` | `String` or `nil` | PEM-encoded client private key for mTLS |
| `api_key:` | `String` or `nil` | API key for Bearer token authentication |

When no TLS/auth options are provided, the client connects over plaintext (backward compatible).

### `client.enqueue(queue:, headers:, payload:)`

Enqueue a message. Returns the broker-assigned message ID (UUIDv7).

### `client.consume(queue:) { |msg| ... }`

Open a streaming consumer. Yields `Fila::ConsumeMessage` objects as they become available. If no block is given, returns an `Enumerator`. Nacked messages are redelivered on the same stream.

### `client.ack(queue:, msg_id:)`

Acknowledge a successfully processed message. The message is permanently removed.

### `client.nack(queue:, msg_id:, error:)`

Negatively acknowledge a failed message. The message is requeued or routed to the dead-letter queue based on the queue's configuration.

### `client.close`

Close the underlying gRPC channel.

## Error Handling

Per-operation error classes are raised for specific failure modes:

```ruby
begin
  client.enqueue(queue: "missing-queue", payload: "test")
rescue Fila::QueueNotFoundError => e
  # handle queue not found
end

begin
  client.ack(queue: "my-queue", msg_id: "missing-id")
rescue Fila::MessageNotFoundError => e
  # handle message not found
end
```

## License

AGPLv3
