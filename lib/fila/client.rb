# frozen_string_literal: true

require 'grpc'

# Add proto directory to load path so generated requires resolve correctly.
$LOAD_PATH.unshift(File.expand_path('proto', __dir__)) unless $LOAD_PATH.include?(File.expand_path('proto', __dir__))

require_relative 'proto/fila/v1/service_services_pb'
require_relative 'errors'
require_relative 'consume_message'

module Fila
  # Client for the Fila message broker.
  #
  # Wraps the hot-path gRPC operations: enqueue, consume, ack, nack.
  #
  # @example Plain-text (no auth)
  #   client = Fila::Client.new("localhost:5555")
  #
  # @example TLS with system trust store
  #   client = Fila::Client.new("localhost:5555", tls: true)
  #
  # @example TLS with custom CA
  #   client = Fila::Client.new("localhost:5555", ca_cert: File.read("ca.pem"))
  #
  # @example mTLS + API key
  #   client = Fila::Client.new("localhost:5555",
  #     ca_cert: File.read("ca.pem"),
  #     client_cert: File.read("client.pem"),
  #     client_key: File.read("client-key.pem"),
  #     api_key: "fila_abc123")
  class Client
    def initialize(addr, tls: false, ca_cert: nil, client_cert: nil, client_key: nil, api_key: nil)
      @api_key = api_key
      @credentials = build_credentials(tls: tls, ca_cert: ca_cert, client_cert: client_cert, client_key: client_key)
      @stub = ::Fila::V1::FilaService::Stub.new(addr, @credentials)
    end

    def close; end

    def enqueue(queue:, payload:, headers: nil)
      req = ::Fila::V1::EnqueueRequest.new(
        queue: queue,
        headers: headers || {},
        payload: payload
      )
      resp = @stub.enqueue(req, metadata: call_metadata)
      resp.message_id
    rescue GRPC::NotFound => e
      raise QueueNotFoundError, "enqueue: #{e.details}"
    rescue GRPC::BadStatus => e
      raise RPCError.new(e.code, e.details)
    end

    # Open a streaming consumer. Yields messages as they arrive.
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

    def consume_with_redirect(queue:, redirected:, &block) # rubocop:disable Metrics/AbcSize
      stream = @stub.consume(::Fila::V1::ConsumeRequest.new(queue: queue), metadata: call_metadata)
      stream.each do |resp|
        msg = resp.message
        next if msg.nil? || msg.id.empty?

        block.call(build_consume_message(msg))
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
