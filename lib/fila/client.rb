# frozen_string_literal: true

require_relative 'errors'
require_relative 'consume_message'
require_relative 'enqueue_result'
require_relative 'batcher'
require_relative 'transport'
require_relative 'codec'

module Fila
  # Client for the Fila message broker over the FIBP (Fila Binary Protocol).
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
      validate_tls_options(tls || ca_cert, client_cert, client_key)

      host, port = parse_addr(addr)
      @transport = Transport.new(
        host: host, port: port,
        tls: tls, ca_cert: ca_cert,
        client_cert: client_cert, client_key: client_key,
        api_key: api_key
      )
      @batcher = start_batcher(batch_mode, max_batch_size, batch_size, linger_ms)
    end

    # Drain pending batched messages and disconnect.
    def close
      @batcher&.close
      @batcher = nil
      @transport&.close
      @transport = nil
    end

    # Enqueue a single message to a queue.
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
    # @raise [RPCError] for unexpected transport failures
    def enqueue(queue:, payload:, headers: nil)
      msg = { queue: queue, payload: payload, headers: headers || {} }

      if @batcher
        @batcher.submit(msg)
      else
        enqueue_single(msg)
      end
    end

    # Enqueue multiple messages in a single RPC call.
    #
    # Each message is independently validated and processed. A failed
    # message does not affect the others. Returns an array of
    # EnqueueResult with one result per input message, in order.
    #
    # This bypasses the background batcher and always uses the
    # Enqueue RPC directly.
    #
    # @param messages [Array<Hash>] messages to enqueue; each hash has
    #   keys :queue (String), :payload (String), and optionally
    #   :headers (Hash<String,String>)
    # @return [Array<EnqueueResult>]
    # @raise [RPCError] for transport-level failures
    def enqueue_many(messages)
      return [] if messages.empty?

      # Group messages by queue for the wire format (all in one frame,
      # queue name is per-message in FIBP).
      # The protocol supports a single queue per frame, so we use the
      # queue of the first message and encode the rest individually by
      # sending as a batch under each unique queue.
      enqueue_many_raw(messages)
    rescue Transport::ConnectionClosed => e
      raise RPCError.new(0, "connection closed: #{e.message}")
    end

    # Open a streaming consumer. Yields messages as they arrive.
    # Returns an Enumerator if no block given.
    #
    # @param queue [String] queue to consume
    # @yield [ConsumeMessage]
    def consume(queue:, &block)
      return enum_for(:consume, queue: queue) unless block

      consume_stream(queue, &block)
    end

    # Acknowledge a successfully processed message.
    #
    # @param queue [String] queue the message belongs to
    # @param msg_id [String] ID of the message to acknowledge
    # @raise [MessageNotFoundError] if the message does not exist
    # @raise [RPCError] for unexpected transport failures
    def ack(queue:, msg_id:)
      payload = Codec.encode_ack([{ queue: queue, msg_id: msg_id }])
      resp    = @transport.request(Transport::OP_ACK, payload)
      results = Codec.decode_ack_response(resp)

      result = results.first
      raise RPCError.new(0, 'no result from server') if result.nil?
      return if result[:ok]

      raise_ack_nack_error(result, 'ack')
    rescue Transport::ConnectionClosed => e
      raise RPCError.new(0, "connection closed: #{e.message}")
    end

    # Negatively acknowledge a message that failed processing.
    #
    # @param queue [String] queue the message belongs to
    # @param msg_id [String] ID of the message to nack
    # @param error [String] description of the failure
    # @raise [MessageNotFoundError] if the message does not exist
    # @raise [RPCError] for unexpected transport failures
    def nack(queue:, msg_id:, error:)
      payload = Codec.encode_nack([{ queue: queue, msg_id: msg_id, error: error }])
      resp    = @transport.request(Transport::OP_NACK, payload)
      results = Codec.decode_nack_response(resp)

      result = results.first
      raise RPCError.new(0, 'no result from server') if result.nil?
      return if result[:ok]

      raise_ack_nack_error(result, 'nack')
    rescue Transport::ConnectionClosed => e
      raise RPCError.new(0, "connection closed: #{e.message}")
    end

    private

    def parse_addr(addr)
      # Support "host:port" and IPv6 "[::1]:5555"
      pattern = addr.start_with?('[') ? /\A\[(.+)\]:(\d+)\z/ : /\A(.+):(\d+)\z/
      m = addr.match(pattern)
      raise ArgumentError, "invalid address #{addr.inspect}, expected host:port" unless m

      [m[1], m[2].to_i]
    end

    def validate_batch_mode(mode)
      return if BATCH_MODES.include?(mode)

      raise ArgumentError, "invalid batch_mode: #{mode.inspect}, must be one of #{BATCH_MODES.inspect}"
    end

    def validate_tls_options(tls_enabled, client_cert, client_key)
      return if tls_enabled || (!client_cert && !client_key)

      raise ArgumentError, 'tls: true or ca_cert is required when client_cert or client_key is provided'
    end

    def start_batcher(mode, max_batch_size, batch_size, linger_ms)
      return nil if mode == :disabled

      Batcher.new(
        transport: @transport,
        mode: mode,
        max_batch_size: max_batch_size,
        batch_size: batch_size,
        linger_ms: linger_ms
      )
    end

    # Send a single message as a batch of one.
    def enqueue_single(msg)
      results = enqueue_many_raw([msg])
      result  = results.first
      raise RPCError.new(0, 'no result from server') if result.nil?

      raise QueueNotFoundError, "enqueue: #{result.error}" unless result.success?

      result.message_id
    rescue Transport::ConnectionClosed => e
      raise RPCError.new(0, "connection closed: #{e.message}")
    end

    # Raw multi-message enqueue — groups by queue, sends one frame per queue.
    def enqueue_many_raw(messages)
      # Group by queue to produce per-queue frames
      groups = messages.each_with_index.group_by { |m, _| m[:queue] }
      # Collect results in original order
      all_results = Array.new(messages.size)

      groups.each do |queue, indexed_msgs|
        msgs_only  = indexed_msgs.map(&:first)
        indices    = indexed_msgs.map(&:last)
        payload    = Codec.encode_enqueue(queue, msgs_only)
        resp       = @transport.request(Transport::OP_ENQUEUE, payload)
        results    = Codec.decode_enqueue_response(resp)

        indices.each_with_index { |orig_idx, i| all_results[orig_idx] = results[i] }
      end

      all_results
    end

    def consume_stream(queue, &block)
      push_q   = Queue.new
      payload  = Codec.encode_consume(queue)
      corr_id  = @transport.start_consume(payload, push_q)

      loop do
        frame = push_q.pop
        case frame
        when Transport::ConnectionClosed then break
        when Exception                   then raise frame
        when String
          msg = Codec.decode_consume_push(frame)
          block.call(msg) if msg
        end
      end
    ensure
      @transport.stop_consume(corr_id) if corr_id
    end

    def raise_ack_nack_error(result, operation)
      case result[:err_code]
      when Transport::ERR_MESSAGE_NOT_FOUND
        raise MessageNotFoundError, "#{operation}: #{result[:err_msg]}"
      else
        raise RPCError.new(result[:err_code], "#{operation}: #{result[:err_msg]}")
      end
    end
  end
end
