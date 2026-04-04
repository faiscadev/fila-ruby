# frozen_string_literal: true

require 'socket'
require 'openssl'
require 'monitor'

module Fila
  module FIBP
    # TCP connection with TLS support, FIBP framing, handshake, and
    # request/response correlation. Supports both synchronous
    # request/response and asynchronous delivery streaming.
    class Connection
      PROTOCOL_VERSION = 1
      DEFAULT_MAX_FRAME_SIZE = 16 * 1024 * 1024 # 16 MiB

      attr_reader :node_id, :max_frame_size

      # @param host [String] server hostname or IP
      # @param port [Integer] server port
      # @param tls [Boolean] enable TLS with system trust store
      # @param ca_cert [String, nil] PEM CA certificate
      # @param client_cert [String, nil] PEM client certificate (mTLS)
      # @param client_key [String, nil] PEM client key (mTLS)
      # @param api_key [String, nil] API key for handshake auth
      def initialize(host:, port:, tls: false, ca_cert: nil, client_cert: nil, client_key: nil, api_key: nil) # rubocop:disable Metrics/ParameterLists
        @host = host
        @port = port
        @api_key = api_key
        @tls_enabled = tls || !ca_cert.nil?
        @ca_cert = ca_cert
        @client_cert = client_cert
        @client_key = client_key

        @request_id_counter = 0
        @pending = {}
        @delivery_callbacks = {}
        @monitor = Monitor.new
        @read_monitor = Monitor.new
        @write_monitor = Monitor.new
        @closed = false
        @continuation_buffers = {}

        connect!
        perform_handshake
        start_reader_thread
      end

      # Send a request frame and wait for the response.
      #
      # @param opcode [Integer] request opcode
      # @param payload [String] encoded payload bytes
      # @return [Array(Integer, String)] [response_opcode, response_payload]
      def request(opcode, payload)
        raise Fila::Error, 'connection closed' if @closed

        rid = next_request_id
        result_queue = Queue.new
        @monitor.synchronize { @pending[rid] = result_queue }

        send_frame(opcode, rid, payload)

        response = result_queue.pop
        raise response if response.is_a?(Exception)

        response
      end

      # Register a delivery callback for a consume subscription.
      #
      # @param request_id [Integer] the consume request ID
      # @param callback [Proc] called with (opcode, payload) for each delivery
      # @return [Integer] the request_id
      def subscribe(opcode, payload, &callback)
        raise Fila::Error, 'connection closed' if @closed

        rid = next_request_id
        result_queue = Queue.new
        @monitor.synchronize do
          @pending[rid] = result_queue
          @delivery_callbacks[rid] = callback
        end

        send_frame(opcode, rid, payload)

        # Wait for ConsumeOk or Error
        response = result_queue.pop
        raise response if response.is_a?(Exception)

        [rid, response]
      end

      # Cancel a consume subscription.
      def cancel_consume(request_id)
        send_frame(Opcodes::CANCEL_CONSUME, request_id, '')
        @monitor.synchronize { @delivery_callbacks.delete(request_id) }
      end

      # Close the connection gracefully.
      def close
        return if @closed

        @closed = true
        begin
          send_frame(Opcodes::DISCONNECT, 0, '')
        rescue StandardError
          nil
        end
        @socket&.close
        @reader_thread&.join(2)

        # Unblock any waiting requests.
        @monitor.synchronize do
          @pending.each_value { |q| q.push(Fila::Error.new('connection closed')) }
          @pending.clear
          @delivery_callbacks.clear
        end
      end

      private

      def connect!
        tcp = TCPSocket.new(@host, @port)
        tcp.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

        @socket = if @tls_enabled
                    wrap_tls(tcp)
                  else
                    tcp
                  end
      end

      def wrap_tls(tcp)
        ctx = OpenSSL::SSL::SSLContext.new
        ctx.min_version = OpenSSL::SSL::TLS1_2_VERSION

        if @ca_cert
          store = OpenSSL::X509::Store.new
          store.add_cert(OpenSSL::X509::Certificate.new(@ca_cert))
          ctx.cert_store = store
        else
          ctx.set_params # uses system trust store
        end

        if @client_cert && @client_key
          ctx.cert = OpenSSL::X509::Certificate.new(@client_cert)
          ctx.key = OpenSSL::PKey.read(@client_key)
        end

        ssl = OpenSSL::SSL::SSLSocket.new(tcp, ctx)
        ssl.hostname = @host
        ssl.sync_close = true
        ssl.connect
        ssl
      end

      def perform_handshake
        payload = Codec.encode_u16(PROTOCOL_VERSION)
        payload += Codec.encode_optional_string(@api_key)

        send_frame(Opcodes::HANDSHAKE, 0, payload)

        opcode, resp_payload = read_frame
        if opcode == Opcodes::ERROR
          reader = Codec::Reader.new(resp_payload)
          code = reader.read_u8
          message = reader.read_string
          raise_protocol_error(code, message, reader)
        end

        unless opcode == Opcodes::HANDSHAKE_OK
          raise ProtocolError, "expected HandshakeOk, got opcode 0x#{opcode.to_s(16)}"
        end

        reader = Codec::Reader.new(resp_payload)
        _version = reader.read_u16
        @node_id = reader.read_u64
        max_frame = reader.read_u32
        @max_frame_size = max_frame.zero? ? DEFAULT_MAX_FRAME_SIZE : max_frame
      end

      def start_reader_thread
        @reader_thread = Thread.new { reader_loop }
        @reader_thread.abort_on_exception = false
      end

      def reader_loop
        until @closed
          opcode, payload, request_id = read_frame_with_id
          break if opcode.nil?

          handle_incoming(opcode, payload, request_id)
        end
      rescue IOError, Errno::ECONNRESET, OpenSSL::SSL::SSLError
        # Connection closed
      ensure
        @closed = true
        @monitor.synchronize do
          err = Fila::Error.new('connection closed')
          @pending.each_value { |q| q.push(err) }
          @pending.clear
        end
      end

      def handle_incoming(opcode, payload, request_id)
        if opcode == Opcodes::PING
          send_frame(Opcodes::PONG, request_id, '')
          return
        end

        if opcode == Opcodes::DELIVERY
          cb = @monitor.synchronize { @delivery_callbacks[request_id] }
          cb&.call(opcode, payload)
          return
        end

        queue = @monitor.synchronize { @pending.delete(request_id) }
        queue&.push([opcode, payload])
      end

      def send_frame(opcode, request_id, payload)
        frame = Codec.encode_frame(opcode, request_id, payload)
        @write_monitor.synchronize { write_all(frame) }
      end

      def write_all(data)
        total = 0
        while total < data.bytesize
          written = @socket.write(data.byteslice(total..))
          total += written
        end
      end

      def read_frame
        opcode, payload, _rid = read_frame_with_id
        [opcode, payload]
      end

      def read_frame_with_id # rubocop:disable Metrics/AbcSize
        @read_monitor.synchronize do
          loop do
            opcode, flags, request_id, payload = read_raw_frame
            return [nil, nil, nil] if opcode.nil?

            next buffer_continuation(request_id, opcode, payload) if flags.anybits?(FLAG_CONTINUATION)

            opcode, payload = reassemble_continuation(request_id, opcode, payload)
            return [opcode, payload, request_id]
          end
        end
      end

      def read_raw_frame
        length_bytes = read_exact(4)
        return [nil, nil, nil, nil] if length_bytes.nil?

        body = read_exact(length_bytes.unpack1('N'))
        return [nil, nil, nil, nil] if body.nil?

        [body.getbyte(0), body.getbyte(1), body.byteslice(2, 4).unpack1('N'), body.byteslice(6..)]
      end

      def buffer_continuation(request_id, opcode, payload)
        @continuation_buffers[request_id] ||= { opcode: opcode, data: +''.b }
        @continuation_buffers[request_id][:data] << payload
      end

      def reassemble_continuation(request_id, opcode, payload)
        if @continuation_buffers.key?(request_id)
          buf = @continuation_buffers.delete(request_id)
          [buf[:opcode], buf[:data] + payload]
        else
          [opcode, payload]
        end
      end

      def read_exact(size)
        buf = +''.b
        while buf.bytesize < size
          chunk = @socket.read(size - buf.bytesize)
          return nil if chunk.nil? || chunk.empty?

          buf << chunk
        end
        buf
      end

      def next_request_id
        @monitor.synchronize do
          @request_id_counter = (@request_id_counter + 1) & 0xFFFFFFFF
          @request_id_counter
        end
      end

      def raise_protocol_error(code, message, reader)
        metadata = reader.remaining.positive? ? reader.read_map : {}
        case code
        when ErrorCodes::UNAUTHORIZED
          raise Fila::AuthenticationError, message
        when ErrorCodes::FORBIDDEN
          raise Fila::ForbiddenError, message
        when ErrorCodes::NOT_LEADER
          leader_addr = metadata['leader_addr']
          raise Fila::NotLeaderError.new(message, leader_addr: leader_addr)
        when ErrorCodes::QUEUE_NOT_FOUND
          raise Fila::QueueNotFoundError, message
        when ErrorCodes::UNSUPPORTED_VERSION
          raise ProtocolError, "unsupported version: #{message}"
        else
          raise Fila::RPCError.new(code, message)
        end
      end
    end
  end
end
