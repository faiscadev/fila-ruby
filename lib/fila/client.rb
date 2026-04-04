# frozen_string_literal: true

require_relative 'errors'
require_relative 'consume_message'
require_relative 'enqueue_result'
require_relative 'batcher'
require_relative 'fibp/opcodes'
require_relative 'fibp/codec'
require_relative 'fibp/connection'

module Fila
  # Client for the Fila message broker over the FIBP binary protocol.
  #
  # @example Plain-text, default auto-batching
  #   client = Fila::Client.new("localhost:5555")
  #
  # @example Batching disabled
  #   client = Fila::Client.new("localhost:5555", batch_mode: :disabled)
  #
  # @example TLS with custom CA
  #   client = Fila::Client.new("localhost:5555",
  #     ca_cert: File.read("ca.pem"))
  #
  # @example mTLS + API key
  #   client = Fila::Client.new("localhost:5555",
  #     ca_cert: File.read("ca.pem"),
  #     client_cert: File.read("client.pem"),
  #     client_key: File.read("client-key.pem"),
  #     api_key: "fila_abc123")
  class Client # rubocop:disable Metrics/ClassLength
    BATCH_MODES = %i[auto linger disabled].freeze

    private_constant :BATCH_MODES

    # @param addr [String] server address (host:port)
    # @param tls [Boolean] enable TLS with system trust store
    # @param ca_cert [String, nil] PEM-encoded CA certificate
    # @param client_cert [String, nil] PEM-encoded client certificate (mTLS)
    # @param client_key [String, nil] PEM-encoded client key (mTLS)
    # @param api_key [String, nil] API key for authentication
    # @param batch_mode [Symbol] :auto (default), :linger, or :disabled
    # @param max_batch_size [Integer] max batch size for auto mode
    # @param batch_size [Integer] batch size for linger mode
    # @param linger_ms [Integer] linger time in ms for linger mode
    def initialize( # rubocop:disable Metrics/ParameterLists
      addr, tls: false, ca_cert: nil, client_cert: nil, client_key: nil,
      api_key: nil, batch_mode: :auto, max_batch_size: 100,
      batch_size: 100, linger_ms: 10
    )
      validate_batch_mode(batch_mode)
      @addr = addr
      @tls = tls
      @ca_cert = ca_cert
      @client_cert = client_cert
      @client_key = client_key
      @api_key = api_key

      @conn = build_connection(addr)
      @batcher = start_batcher(batch_mode, max_batch_size, batch_size, linger_ms)
    end

    # Drain pending batched messages and disconnect.
    def close
      @batcher&.close
      @batcher = nil
      @conn&.close
      @conn = nil
    end

    # Enqueue a single message to a queue.
    #
    # @param queue [String] target queue name
    # @param payload [String] message payload
    # @param headers [Hash<String,String>, nil] optional headers
    # @return [String] broker-assigned message ID
    def enqueue(queue:, payload:, headers: nil)
      msg = { queue: queue, headers: headers || {}, payload: payload }

      if @batcher
        @batcher.submit(msg)
      else
        enqueue_single(msg)
      end
    end

    # Enqueue multiple messages in a single request.
    #
    # @param messages [Array<Hash>] messages with :queue, :payload, :headers
    # @return [Array<EnqueueResult>]
    def enqueue_many(messages)
      return [] if messages.empty?

      payload = encode_enqueue_batch(messages)
      opcode, resp = @conn.request(FIBP::Opcodes::ENQUEUE, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      decode_enqueue_results(resp)
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
    def ack(queue:, msg_id:)
      payload = FIBP::Codec.encode_u32(1) +
                FIBP::Codec.encode_string(queue) +
                FIBP::Codec.encode_string(msg_id)
      opcode, resp = @conn.request(FIBP::Opcodes::ACK, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      count = reader.read_u32
      raise RPCError.new(0xFF, 'no result from server') if count.zero?

      code = reader.read_u8
      return if code == FIBP::ErrorCodes::OK

      case code
      when FIBP::ErrorCodes::MESSAGE_NOT_FOUND
        raise MessageNotFoundError, 'ack: message not found'
      else
        raise RPCError.new(code, 'ack failed')
      end
    end

    # Negatively acknowledge a message that failed processing.
    #
    # @param queue [String] queue the message belongs to
    # @param msg_id [String] ID of the message to nack
    # @param error [String] description of the failure
    def nack(queue:, msg_id:, error:)
      payload = FIBP::Codec.encode_u32(1) +
                FIBP::Codec.encode_string(queue) +
                FIBP::Codec.encode_string(msg_id) +
                FIBP::Codec.encode_string(error)
      opcode, resp = @conn.request(FIBP::Opcodes::NACK, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      count = reader.read_u32
      raise RPCError.new(0xFF, 'no result from server') if count.zero?

      code = reader.read_u8
      return if code == FIBP::ErrorCodes::OK

      case code
      when FIBP::ErrorCodes::MESSAGE_NOT_FOUND
        raise MessageNotFoundError, 'nack: message not found'
      else
        raise RPCError.new(code, 'nack failed')
      end
    end

    # --- Admin operations ---

    # Create a queue.
    #
    # @param name [String] queue name
    # @param on_enqueue_script [String, nil] Lua enqueue script
    # @param on_failure_script [String, nil] Lua failure script
    # @param visibility_timeout_ms [Integer] visibility timeout (0 = server default)
    # @return [String] created queue ID
    def create_queue(name:, on_enqueue_script: nil, on_failure_script: nil, visibility_timeout_ms: 0)
      payload = FIBP::Codec.encode_string(name) +
                FIBP::Codec.encode_optional_string(on_enqueue_script) +
                FIBP::Codec.encode_optional_string(on_failure_script) +
                FIBP::Codec.encode_u64(visibility_timeout_ms)
      opcode, resp = @conn.request(FIBP::Opcodes::CREATE_QUEUE, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      queue_id = reader.read_string

      case code
      when FIBP::ErrorCodes::OK then queue_id
      when FIBP::ErrorCodes::QUEUE_ALREADY_EXISTS
        raise QueueAlreadyExistsError, "queue '#{name}' already exists"
      else
        raise RPCError.new(code, 'create queue failed')
      end
    end

    # Delete a queue.
    #
    # @param queue [String] queue name
    def delete_queue(queue:)
      payload = FIBP::Codec.encode_string(queue)
      opcode, resp = @conn.request(FIBP::Opcodes::DELETE_QUEUE, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      return if code == FIBP::ErrorCodes::OK

      raise_for_error_code(code, 'delete queue')
    end

    # Get statistics for a queue.
    #
    # @param queue [String] queue name
    # @return [Hash] queue statistics
    def get_stats(queue:)
      payload = FIBP::Codec.encode_string(queue)
      opcode, resp = @conn.request(FIBP::Opcodes::GET_STATS, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      decode_stats_result(resp)
    end

    # List all queues.
    #
    # @return [Hash] with :cluster_node_count and :queues array
    def list_queues
      opcode, resp = @conn.request(FIBP::Opcodes::LIST_QUEUES, '')

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      decode_list_queues_result(resp)
    end

    # Set a runtime config key.
    def set_config(key:, value:)
      payload = FIBP::Codec.encode_string(key) + FIBP::Codec.encode_string(value)
      opcode, resp = @conn.request(FIBP::Opcodes::SET_CONFIG, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      return if code == FIBP::ErrorCodes::OK

      raise_for_error_code(code, 'set config')
    end

    # Get a runtime config value.
    def get_config(key:)
      payload = FIBP::Codec.encode_string(key)
      opcode, resp = @conn.request(FIBP::Opcodes::GET_CONFIG, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      raise_for_error_code(code, 'get config') unless code == FIBP::ErrorCodes::OK

      reader.read_string
    end

    # List config keys by prefix.
    def list_config(prefix:)
      payload = FIBP::Codec.encode_string(prefix)
      opcode, resp = @conn.request(FIBP::Opcodes::LIST_CONFIG, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      raise_for_error_code(code, 'list config') unless code == FIBP::ErrorCodes::OK

      count = reader.read_u16
      Array.new(count) do
        { key: reader.read_string, value: reader.read_string }
      end
    end

    # Redrive messages from a DLQ back to their parent queue.
    def redrive(dlq_queue:, count:)
      payload = FIBP::Codec.encode_string(dlq_queue) + FIBP::Codec.encode_u64(count)
      opcode, resp = @conn.request(FIBP::Opcodes::REDRIVE, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      raise_for_error_code(code, 'redrive') unless code == FIBP::ErrorCodes::OK

      reader.read_u64
    end

    # --- Auth operations ---

    # Create an API key.
    def create_api_key(name:, expires_at_ms: 0, is_superadmin: false)
      payload = FIBP::Codec.encode_string(name) +
                FIBP::Codec.encode_u64(expires_at_ms) +
                FIBP::Codec.encode_bool(is_superadmin)
      opcode, resp = @conn.request(FIBP::Opcodes::CREATE_API_KEY, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      raise_for_error_code(code, 'create api key') unless code == FIBP::ErrorCodes::OK

      { key_id: reader.read_string, key: reader.read_string, is_superadmin: reader.read_bool }
    end

    # Revoke an API key.
    def revoke_api_key(key_id:)
      payload = FIBP::Codec.encode_string(key_id)
      opcode, resp = @conn.request(FIBP::Opcodes::REVOKE_API_KEY, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      return if code == FIBP::ErrorCodes::OK

      raise_for_error_code(code, 'revoke api key')
    end

    # List all API keys.
    def list_api_keys
      opcode, resp = @conn.request(FIBP::Opcodes::LIST_API_KEYS, '')

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      raise_for_error_code(code, 'list api keys') unless code == FIBP::ErrorCodes::OK

      count = reader.read_u16
      Array.new(count) do
        {
          key_id: reader.read_string, name: reader.read_string,
          created_at_ms: reader.read_u64, expires_at_ms: reader.read_u64,
          is_superadmin: reader.read_bool
        }
      end
    end

    # Set ACL permissions for an API key.
    def set_acl(key_id:, permissions:)
      payload = FIBP::Codec.encode_string(key_id) +
                FIBP::Codec.encode_u16(permissions.size)
      permissions.each do |perm|
        payload += FIBP::Codec.encode_string(perm[:kind]) +
                   FIBP::Codec.encode_string(perm[:pattern])
      end
      opcode, resp = @conn.request(FIBP::Opcodes::SET_ACL, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      return if code == FIBP::ErrorCodes::OK

      raise_for_error_code(code, 'set acl')
    end

    # Get ACL permissions for an API key.
    def get_acl(key_id:)
      payload = FIBP::Codec.encode_string(key_id)
      opcode, resp = @conn.request(FIBP::Opcodes::GET_ACL, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      raise_for_error_code(code, 'get acl') unless code == FIBP::ErrorCodes::OK

      key_id_resp = reader.read_string
      is_superadmin = reader.read_bool
      perm_count = reader.read_u16
      permissions = Array.new(perm_count) do
        { kind: reader.read_string, pattern: reader.read_string }
      end
      { key_id: key_id_resp, is_superadmin: is_superadmin, permissions: permissions }
    end

    ERROR_CODE_TO_CLASS = {
      FIBP::ErrorCodes::QUEUE_NOT_FOUND => QueueNotFoundError,
      FIBP::ErrorCodes::MESSAGE_NOT_FOUND => MessageNotFoundError,
      FIBP::ErrorCodes::QUEUE_ALREADY_EXISTS => QueueAlreadyExistsError,
      FIBP::ErrorCodes::UNAUTHORIZED => AuthenticationError,
      FIBP::ErrorCodes::FORBIDDEN => ForbiddenError,
      FIBP::ErrorCodes::API_KEY_NOT_FOUND => ApiKeyNotFoundError
    }.freeze

    private_constant :ERROR_CODE_TO_CLASS

    private

    def validate_batch_mode(mode)
      return if BATCH_MODES.include?(mode)

      raise ArgumentError, "invalid batch_mode: #{mode.inspect}, must be one of #{BATCH_MODES.inspect}"
    end

    def build_connection(addr)
      host, port_str = addr.split(':')
      port = port_str.to_i
      validate_tls_options
      FIBP::Connection.new(
        host: host, port: port,
        tls: @tls, ca_cert: @ca_cert,
        client_cert: @client_cert, client_key: @client_key,
        api_key: @api_key
      )
    end

    def validate_tls_options
      tls_enabled = @tls || @ca_cert
      return if tls_enabled || (!@client_cert && !@client_key)

      raise ArgumentError, 'tls: true or ca_cert is required when client_cert or client_key is provided'
    end

    def start_batcher(mode, max_batch_size, batch_size, linger_ms)
      return nil if mode == :disabled

      Batcher.new(
        conn: @conn,
        mode: mode,
        max_batch_size: max_batch_size,
        batch_size: batch_size,
        linger_ms: linger_ms
      )
    end

    def enqueue_single(msg)
      payload = encode_enqueue_batch([msg])
      opcode, resp = @conn.request(FIBP::Opcodes::ENQUEUE, payload)

      raise_from_error_frame(resp) if opcode == FIBP::Opcodes::ERROR

      reader = FIBP::Codec::Reader.new(resp)
      count = reader.read_u32
      raise RPCError.new(0xFF, 'no result from server') if count.zero?

      code = reader.read_u8
      msg_id = reader.read_string

      case code
      when FIBP::ErrorCodes::OK then msg_id
      when FIBP::ErrorCodes::QUEUE_NOT_FOUND
        raise QueueNotFoundError, 'enqueue: queue not found'
      else
        raise RPCError.new(code, 'enqueue failed')
      end
    end

    def encode_enqueue_batch(messages)
      payload = FIBP::Codec.encode_u32(messages.size)
      messages.each do |m|
        payload << FIBP::Codec.encode_string(m[:queue])
        payload << FIBP::Codec.encode_map(m[:headers] || {})
        payload << FIBP::Codec.encode_bytes(m[:payload])
      end
      payload
    end

    def decode_enqueue_results(resp)
      reader = FIBP::Codec::Reader.new(resp)
      count = reader.read_u32
      Array.new(count) do
        code = reader.read_u8
        msg_id = reader.read_string
        if code == FIBP::ErrorCodes::OK
          EnqueueResult.new(message_id: msg_id)
        else
          EnqueueResult.new(error: error_name(code))
        end
      end
    end

    def consume_with_redirect(queue:, redirected:, &)
      payload = FIBP::Codec.encode_string(queue)
      delivery_queue = Queue.new
      done = [false] # mutable container for closure capture
      rid = nil

      rid, response = subscribe_to_queue(payload, delivery_queue, done)
      check_consume_response(response)
      consume_delivery_loop(delivery_queue, &)
    rescue NotLeaderError => e
      rid = handle_not_leader_redirect(e, rid, done, redirected, queue, &)
    rescue LocalJumpError
      nil # Consumer break
    ensure
      done[0] = true
      @conn&.cancel_consume(rid) if rid
    end

    def handle_not_leader_redirect(error, rid, done, redirected, queue, &)
      raise error if redirected || error.leader_addr.nil?

      done[0] = true
      @conn&.cancel_consume(rid) if rid
      reconnect_to(error.leader_addr)
      consume_with_redirect(queue: queue, redirected: true, &)
      nil
    end

    def subscribe_to_queue(payload, delivery_queue, done)
      @conn.subscribe(FIBP::Opcodes::CONSUME, payload) do |_opcode, del_payload|
        delivery_queue.push(del_payload) unless done[0]
      end
    end

    def check_consume_response(response)
      opcode, resp_payload = response
      raise_from_error_frame(resp_payload) if opcode == FIBP::Opcodes::ERROR
    end

    def consume_delivery_loop(delivery_queue, &block)
      loop do
        del_payload = delivery_queue.pop
        break if del_payload.nil?

        process_delivery(del_payload, &block)
      end
    end

    def process_delivery(payload, &block)
      reader = FIBP::Codec::Reader.new(payload)
      count = reader.read_u32
      count.times do
        msg = decode_delivery_message(reader)
        block.call(msg)
      end
    end

    def decode_delivery_message(reader)
      msg_id = reader.read_string
      queue = reader.read_string
      headers = reader.read_map
      payload = reader.read_bytes
      fairness_key = reader.read_string
      _weight = reader.read_u32
      _throttle_keys = reader.read_string_array
      attempt_count = reader.read_u32
      _enqueued_at = reader.read_u64
      _leased_at = reader.read_u64

      ConsumeMessage.new(
        id: msg_id,
        headers: headers,
        payload: payload,
        fairness_key: fairness_key,
        attempt_count: attempt_count,
        queue: queue
      )
    end

    def reconnect_to(addr)
      @conn&.close
      @conn = build_connection(addr)
      @batcher.conn = @conn if @batcher
    end

    def raise_from_error_frame(resp)
      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      message = reader.read_string
      metadata = reader.remaining.positive? ? reader.read_map : {}

      raise NotLeaderError.new(message, leader_addr: metadata['leader_addr']) if code == FIBP::ErrorCodes::NOT_LEADER

      klass = ERROR_CODE_TO_CLASS[code]
      raise klass, message if klass

      raise RPCError.new(code, message)
    end

    def raise_for_error_code(code, context)
      case code
      when FIBP::ErrorCodes::QUEUE_NOT_FOUND
        raise QueueNotFoundError, "#{context}: queue not found"
      when FIBP::ErrorCodes::MESSAGE_NOT_FOUND
        raise MessageNotFoundError, "#{context}: message not found"
      when FIBP::ErrorCodes::QUEUE_ALREADY_EXISTS
        raise QueueAlreadyExistsError, "#{context}: queue already exists"
      when FIBP::ErrorCodes::UNAUTHORIZED
        raise AuthenticationError, "#{context}: unauthorized"
      when FIBP::ErrorCodes::FORBIDDEN
        raise ForbiddenError, "#{context}: forbidden"
      when FIBP::ErrorCodes::API_KEY_NOT_FOUND
        raise ApiKeyNotFoundError, "#{context}: api key not found"
      else
        raise RPCError.new(code, "#{context} failed")
      end
    end

    def decode_stats_result(resp)
      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      raise_for_error_code(code, 'get stats') unless code == FIBP::ErrorCodes::OK

      result = read_stats_base(reader)
      result[:per_key_stats] = read_per_key_stats(reader)
      result[:per_throttle_stats] = read_per_throttle_stats(reader)
      result
    end

    def read_stats_base(reader)
      {
        depth: reader.read_u64, in_flight: reader.read_u64,
        active_fairness_keys: reader.read_u64, active_consumers: reader.read_u32,
        quantum: reader.read_u32, leader_node_id: reader.read_u64,
        replication_count: reader.read_u32
      }
    end

    def read_per_key_stats(reader)
      Array.new(reader.read_u16) do
        { key: reader.read_string, pending_count: reader.read_u64,
          current_deficit: reader.read_i64, weight: reader.read_u32 }
      end
    end

    def read_per_throttle_stats(reader)
      Array.new(reader.read_u16) do
        { key: reader.read_string, tokens: reader.read_f64,
          rate_per_second: reader.read_f64, burst: reader.read_f64 }
      end
    end

    def decode_list_queues_result(resp)
      reader = FIBP::Codec::Reader.new(resp)
      code = reader.read_u8
      raise_for_error_code(code, 'list queues') unless code == FIBP::ErrorCodes::OK

      cluster_node_count = reader.read_u32
      queue_count = reader.read_u16
      queues = Array.new(queue_count) do
        {
          name: reader.read_string,
          depth: reader.read_u64,
          in_flight: reader.read_u64,
          active_consumers: reader.read_u32,
          leader_node_id: reader.read_u64
        }
      end

      { cluster_node_count: cluster_node_count, queues: queues }
    end

    def error_name(code)
      case code
      when FIBP::ErrorCodes::QUEUE_NOT_FOUND then 'queue not found'
      when FIBP::ErrorCodes::MESSAGE_NOT_FOUND then 'message not found'
      when FIBP::ErrorCodes::UNAUTHORIZED then 'unauthorized'
      when FIBP::ErrorCodes::FORBIDDEN then 'forbidden'
      else "error code 0x#{code.to_s(16).rjust(2, '0')}"
      end
    end
  end
end
