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
  # @example
  #   client = Fila::Client.new("localhost:5555")
  #   msg_id = client.enqueue(queue: "my-queue", headers: { "tenant" => "acme" }, payload: "hello")
  #   client.consume(queue: "my-queue") do |msg|
  #     client.ack(queue: "my-queue", msg_id: msg.id)
  #     break
  #   end
  #   client.close
  class Client
    # Connect to a Fila broker at the given address.
    #
    # @param addr [String] broker address in "host:port" format (e.g., "localhost:5555")
    def initialize(addr)
      @stub = ::Fila::V1::FilaService::Stub.new(addr, :this_channel_is_insecure)
    end

    # Close the underlying gRPC channel.
    def close
      # grpc-ruby doesn't expose a direct channel close on stubs;
      # the channel is garbage-collected. This is a no-op for API symmetry.
    end

    # Enqueue a message to the specified queue.
    #
    # @param queue [String] target queue name
    # @param headers [Hash<String, String>, nil] optional message headers
    # @param payload [String] message payload bytes
    # @return [String] broker-assigned message ID (UUIDv7)
    # @raise [QueueNotFoundError] if the queue does not exist
    # @raise [RPCError] for unexpected gRPC failures
    def enqueue(queue:, payload:, headers: nil)
      req = ::Fila::V1::EnqueueRequest.new(
        queue: queue,
        headers: headers || {},
        payload: payload
      )
      resp = @stub.enqueue(req)
      resp.message_id
    rescue GRPC::NotFound => e
      raise QueueNotFoundError, "enqueue: #{e.details}"
    rescue GRPC::BadStatus => e
      raise RPCError.new(e.code, e.details)
    end

    # Open a streaming consumer on the specified queue.
    #
    # Yields messages as they become available. Nil message frames (keepalive
    # signals) are skipped automatically. Nacked messages are redelivered on
    # the same stream.
    #
    # If no block is given, returns an Enumerator.
    #
    # @param queue [String] queue to consume from
    # @yield [ConsumeMessage] each message received from the broker
    # @return [Enumerator<ConsumeMessage>] if no block given
    # @raise [QueueNotFoundError] if the queue does not exist
    # @raise [RPCError] for unexpected gRPC failures
    def consume(queue:, &block)
      return enum_for(:consume, queue: queue) unless block

      req = ::Fila::V1::ConsumeRequest.new(queue: queue)
      stream = @stub.consume(req)
      stream.each do |resp|
        msg = resp.message
        next if msg.nil? || msg.id.empty?

        block.call(build_consume_message(msg))
      end
    rescue GRPC::Cancelled
      # Stream cancelled — normal when consumer breaks out of the loop.
    rescue GRPC::NotFound => e
      raise QueueNotFoundError, "consume: #{e.details}"
    rescue GRPC::BadStatus => e
      raise RPCError.new(e.code, e.details)
    end

    # Acknowledge a successfully processed message.
    #
    # @param queue [String] queue the message belongs to
    # @param msg_id [String] ID of the message to acknowledge
    # @raise [MessageNotFoundError] if the message does not exist
    # @raise [RPCError] for unexpected gRPC failures
    def ack(queue:, msg_id:)
      req = ::Fila::V1::AckRequest.new(queue: queue, message_id: msg_id)
      @stub.ack(req)
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
      @stub.nack(req)
      nil
    rescue GRPC::NotFound => e
      raise MessageNotFoundError, "nack: #{e.details}"
    rescue GRPC::BadStatus => e
      raise RPCError.new(e.code, e.details)
    end

    private

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
