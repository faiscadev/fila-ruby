# frozen_string_literal: true

require 'grpc'

# Add proto directory to load path so generated requires resolve correctly.
$LOAD_PATH.unshift(File.expand_path('proto', __dir__)) unless $LOAD_PATH.include?(File.expand_path('proto', __dir__))

require_relative 'proto/fila/v1/service_services_pb'
require_relative 'errors'
require_relative 'consume_message'
require_relative 'batch_enqueue_result'
require_relative 'batcher'

module Fila
  # Client for the Fila message broker.
  #
  # Wraps the hot-path gRPC operations: enqueue, consume, ack, nack.
  #
  # @example Plain-text, default auto-batching
  #   client = Fila::Client.new("localhost:5555")
  #
  # @example Batching disabled
  #   client = Fila::Client.new("localhost:5555", batch_mode: :disabled)
  #
  # @example Linger-based batching
  #   client = Fila::Client.new("localhost:5555",
  #     batch_mode: :linger, linger_ms: 10, batch_size: 50)
  #
  # @example TLS with system trust store
  #   client = Fila::Client.new("localhost:5555", tls: true)
  #
  # @example mTLS + API key
  #   client = Fila::Client.new("localhost:5555",
  #     ca_cert: File.read("ca.pem"),
  #     client_cert: File.read("client.pem"),
  #     client_key: File.read("client-key.pem"),
  #     api_key: "fila_abc123")
  class Client # rubocop:disable Metrics/ClassLength
    # Valid batch mode values.
    BATCH_MODES = %i[auto linger disabled].freeze

    private_constant :BATCH_MODES

    # @param addr [String] server address (host:port)
    # @param tls [Boolean] enable TLS with system trust store
    # @param ca_cert [String, nil] PEM-encoded CA certificate
    # @param client_cert [String, nil] PEM-encoded client certificate (mTLS)
    # @param client_key [String, nil] PEM-encoded client key (mTLS)
    # @param api_key [String, nil] API key for authentication
    # @param batch_mode [Symbol] :auto (default), :linger, or :disabled
    # @param max_batch_size [Integer] max batch size for auto mode (default: 100)
    # @param batch_size [Integer] batch size for linger mode (default: 100)
    # @param linger_ms [Integer] linger time in ms for linger mode (default: 10)
    def initialize( # rubocop:disable Metrics/ParameterLists
      addr, tls: false, ca_cert: nil, client_cert: nil, client_key: nil,
      api_key: nil, batch_mode: :auto, max_batch_size: 100,
      batch_size: 100, linger_ms: 10
    )
      validate_batch_mode(batch_mode)
      @api_key = api_key
      @credentials = build_credentials(tls: tls, ca_cert: ca_cert, client_cert: client_cert, client_key: client_key)
      @stub = ::Fila::V1::FilaService::Stub.new(addr, @credentials)
      @batcher = start_batcher(batch_mode, max_batch_size, batch_size, linger_ms)
    end

    # Drain pending batched messages and disconnect.
    def close
      @batcher&.close
      @batcher = nil
    end

    # Enqueue a message to a queue.
    #
    # When batching is enabled (default), the message is submitted to
    # the background batcher. At low load each message is sent
    # individually; at high load messages cluster into batches.
    #
    # @param queue [String] target queue name
    # @param payload [String] message payload
    # @param headers [Hash<String,String>, nil] optional headers
    # @return [String] broker-assigned message ID
    # @raise [QueueNotFoundError] if the queue does not exist
    # @raise [RPCError] for unexpected gRPC failures
    def enqueue(queue:, payload:, headers: nil)
      req = ::Fila::V1::EnqueueRequest.new(
        queue: queue,
        headers: headers || {},
        payload: payload
      )

      if @batcher
        @batcher.submit(req)
      else
        enqueue_direct(req)
      end
    end

    # Enqueue a batch of messages in a single RPC call.
    #
    # Each message is independently validated and processed. A failed
    # message does not affect the others. Returns an array of
    # BatchEnqueueResult with one result per input message, in order.
    #
    # This bypasses the background batcher and always uses the
    # BatchEnqueue RPC directly.
    #
    # @param messages [Array<Hash>] messages to enqueue; each hash has
    #   keys :queue (String), :payload (String), and optionally
    #   :headers (Hash<String,String>)
    # @return [Array<BatchEnqueueResult>]
    # @raise [RPCError] for transport-level gRPC failures
    def batch_enqueue(messages)
      proto_messages = messages.map do |m|
        ::Fila::V1::EnqueueRequest.new(
          queue: m[:queue],
          headers: m[:headers] || {},
          payload: m[:payload]
        )
      end

      req = ::Fila::V1::BatchEnqueueRequest.new(messages: proto_messages)
      resp = @stub.batch_enqueue(req, metadata: call_metadata)

      resp.results.map do |r|
        if r.result == :success
          BatchEnqueueResult.new(message_id: r.success.message_id)
        else
          BatchEnqueueResult.new(error: r.error)
        end
      end
    rescue GRPC::BadStatus => e
      raise RPCError.new(e.code, e.details)
    end

    # Open a streaming consumer. Yields messages as they arrive.
    # Transparently unpacks batched delivery (repeated messages field)
    # with fallback to singular message field.
    # Returns an Enumerator if no block given.
    def consume(queue:, &block)
      return enum_for(:consume, queue: queue) unless block

      consume_with_redirect(queue: queue, redirected: false, &block)
    end

    # Acknowledge a successfully processed message.
    #
    # @param queue [String] queue the message belongs to
    # @param msg_id [String] ID of the message to acknowledge
    # @raise [MessageNotFoundError] if the message does not exist
    # @raise [RPCError] for unexpected gRPC failures
    def ack(queue:, msg_id:)
      req = ::Fila::V1::AckRequest.new(queue: queue, message_id: msg_id)
      @stub.ack(req, metadata: call_metadata)
      nil
    rescue GRPC::NotFound => e
      raise MessageNotFoundError, "ack: #{e.details}"
    rescue GRPC::BadStatus => e
      raise RPCError.new(e.code, e.details)
    end

    # Negatively acknowledge a message that failed processing.
    #
    # @param queue [String] queue the message belongs to
    # @param msg_id [String] ID of the message to nack
    # @param error [String] description of the failure
    # @raise [MessageNotFoundError] if the message does not exist
    # @raise [RPCError] for unexpected gRPC failures
    def nack(queue:, msg_id:, error:)
      req = ::Fila::V1::NackRequest.new(queue: queue, message_id: msg_id, error: error)
      @stub.nack(req, metadata: call_metadata)
      nil
    rescue GRPC::NotFound => e
      raise MessageNotFoundError, "nack: #{e.details}"
    rescue GRPC::BadStatus => e
      raise RPCError.new(e.code, e.details)
    end

    LEADER_ADDR_KEY = 'x-fila-leader-addr'

    private_constant :LEADER_ADDR_KEY

    private

    def validate_batch_mode(mode)
      return if BATCH_MODES.include?(mode)

      raise ArgumentError, "invalid batch_mode: #{mode.inspect}, must be one of #{BATCH_MODES.inspect}"
    end

    def start_batcher(mode, max_batch_size, batch_size, linger_ms)
      return nil if mode == :disabled

      Batcher.new(
        stub: @stub,
        metadata: call_metadata,
        mode: mode,
        max_batch_size: max_batch_size,
        batch_size: batch_size,
        linger_ms: linger_ms
      )
    end

    def enqueue_direct(req)
      resp = @stub.enqueue(req, metadata: call_metadata)
      resp.message_id
    rescue GRPC::NotFound => e
      raise QueueNotFoundError, "enqueue: #{e.details}"
    rescue GRPC::BadStatus => e
      raise RPCError.new(e.code, e.details)
    end

    def consume_with_redirect(queue:, redirected:, &block)
      stream = @stub.consume(::Fila::V1::ConsumeRequest.new(queue: queue), metadata: call_metadata)
      stream.each do |resp|
        yield_messages_from_response(resp, &block)
      end
    rescue GRPC::Cancelled then nil
    rescue GRPC::NotFound => e
      raise QueueNotFoundError, "consume: #{e.details}"
    rescue GRPC::Unavailable => e
      raise RPCError.new(e.code, e.details) if (leader_addr = extract_leader_addr(e)).nil? || redirected

      @stub = ::Fila::V1::FilaService::Stub.new(leader_addr, @credentials)
      consume_with_redirect(queue: queue, redirected: true, &block)
    rescue GRPC::BadStatus => e
      raise RPCError.new(e.code, e.details)
    end

    # Unpack messages from a ConsumeResponse. Prefers the repeated
    # messages field (batched delivery); falls back to singular message
    # field for backward compatibility with older servers.
    def yield_messages_from_response(resp, &block)
      msgs = resp.messages
      if msgs && !msgs.empty?
        msgs.each do |msg|
          next if msg.nil? || msg.id.empty?

          block.call(build_consume_message(msg))
        end
      else
        msg = resp.message
        return if msg.nil? || msg.id.empty?

        block.call(build_consume_message(msg))
      end
    end

    def extract_leader_addr(err)
      err.metadata[LEADER_ADDR_KEY]
    rescue StandardError
      nil
    end

    def build_credentials(tls:, ca_cert:, client_cert:, client_key:)
      tls_enabled = tls || ca_cert
      validate_tls_options(tls_enabled, client_cert, client_key)
      return :this_channel_is_insecure unless tls_enabled

      build_channel_credentials(ca_cert, client_cert, client_key)
    end

    def validate_tls_options(tls_enabled, client_cert, client_key)
      return if tls_enabled || (!client_cert && !client_key)

      raise ArgumentError, 'tls: true or ca_cert is required when client_cert or client_key is provided'
    end

    def build_channel_credentials(ca_cert, client_cert, client_key)
      if ca_cert then GRPC::Core::ChannelCredentials.new(ca_cert, client_key, client_cert)
      elsif client_cert && client_key then GRPC::Core::ChannelCredentials.new(nil, client_key, client_cert)
      else GRPC::Core::ChannelCredentials.new
      end
    end

    def call_metadata
      return {} unless @api_key

      { 'authorization' => "Bearer #{@api_key}" }
    end

    def build_consume_message(msg)
      metadata = msg.metadata
      ConsumeMessage.new(
        id: msg.id,
        headers: msg.headers.to_h,
        payload: msg.payload,
        fairness_key: metadata&.fairness_key.to_s,
        attempt_count: metadata&.attempt_count.to_i,
        queue: metadata&.queue_id.to_s
      )
    end
  end
end
