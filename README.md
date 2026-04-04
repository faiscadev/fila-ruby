# fila-ruby

Ruby client SDK for the [Fila](https://github.com/faisca/fila) message broker using the FIBP binary protocol.

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

### TLS (custom CA)

```ruby
client = Fila::Client.new("localhost:5555",
  ca_cert: File.read("ca.pem")
)
```

### mTLS (mutual TLS)

```ruby
client = Fila::Client.new("localhost:5555",
  ca_cert: File.read("ca.pem"),
  client_cert: File.read("client.pem"),
  client_key: File.read("client-key.pem")
)
```

### API Key Authentication

```ruby
client = Fila::Client.new("localhost:5555",
  api_key: "fila_your_api_key_here"
)
```

### mTLS + API Key

```ruby
client = Fila::Client.new("localhost:5555",
  ca_cert: File.read("ca.pem"),
  client_cert: File.read("client.pem"),
  client_key: File.read("client-key.pem"),
  api_key: "fila_your_api_key_here"
)
```

### Batch Enqueue

```ruby
results = client.enqueue_many([
  { queue: "orders", payload: "order-1", headers: { "tenant" => "acme" } },
  { queue: "orders", payload: "order-2" },
])

results.each do |r|
  if r.success?
    puts "Enqueued: #{r.message_id}"
  else
    puts "Failed: #{r.error}"
  end
end
```

### Admin Operations

```ruby
# Create a queue.
client.create_queue(name: "my-queue")

# Delete a queue.
client.delete_queue(queue: "my-queue")

# Get queue statistics.
stats = client.get_stats(queue: "my-queue")

# List all queues.
queues = client.list_queues

# Runtime configuration.
client.set_config(key: "queues.my-queue.visibility_timeout_ms", value: "30000")
value = client.get_config(key: "queues.my-queue.visibility_timeout_ms")
entries = client.list_config(prefix: "queues.")

# Redrive DLQ messages.
count = client.redrive(dlq_queue: "my-queue-dlq", count: 100)
```

### Auth Operations

```ruby
# Create an API key.
result = client.create_api_key(name: "my-key", is_superadmin: false)
puts result[:key]

# Revoke an API key.
client.revoke_api_key(key_id: result[:key_id])

# List API keys.
keys = client.list_api_keys

# Set ACL permissions.
client.set_acl(key_id: "key-id", permissions: [
  { kind: "produce", pattern: "orders.*" },
  { kind: "consume", pattern: "orders.*" },
])

# Get ACL permissions.
acl = client.get_acl(key_id: "key-id")
```

## API

### `Fila::Client.new(addr, ...)`

Connect to a Fila broker at the given address (e.g., `"localhost:5555"`).

| Parameter | Type | Description |
|---|---|---|
| `addr` | `String` | Broker address in `"host:port"` format |
| `tls:` | `Boolean` | Enable TLS using the OS system trust store (default: `false`) |
| `ca_cert:` | `String` or `nil` | PEM-encoded CA certificate for TLS (implies `tls: true`) |
| `client_cert:` | `String` or `nil` | PEM-encoded client certificate for mTLS |
| `client_key:` | `String` or `nil` | PEM-encoded client private key for mTLS |
| `api_key:` | `String` or `nil` | API key sent during FIBP handshake |
| `batch_mode:` | `Symbol` | `:auto` (default), `:linger`, or `:disabled` |

### `client.enqueue(queue:, payload:, headers: nil)`

Enqueue a message. Returns the broker-assigned message ID (UUIDv7).

### `client.enqueue_many(messages)`

Enqueue multiple messages in a single request. Returns an array of `Fila::EnqueueResult`.

### `client.consume(queue:) { |msg| ... }`

Open a streaming consumer. Yields `Fila::ConsumeMessage` objects. If no block is given, returns an `Enumerator`.

### `client.ack(queue:, msg_id:)`

Acknowledge a successfully processed message.

### `client.nack(queue:, msg_id:, error:)`

Negatively acknowledge a failed message.

### `client.close`

Drain pending batches and close the TCP connection.

## Error Handling

Per-operation error classes are raised for specific failure modes:

| Error Class | Description |
|---|---|
| `Fila::QueueNotFoundError` | Queue does not exist |
| `Fila::MessageNotFoundError` | Message not found or not leased |
| `Fila::QueueAlreadyExistsError` | Queue already exists |
| `Fila::AuthenticationError` | Missing or invalid API key |
| `Fila::ForbiddenError` | Insufficient permissions |
| `Fila::NotLeaderError` | Not the leader (includes leader hint) |
| `Fila::RPCError` | Transport or protocol failure |

## License

AGPLv3
