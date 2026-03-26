# frozen_string_literal: true

require 'socket'
require 'openssl'

module Fila
  # Low-level FIBP (Fila Binary Protocol) transport.
  #
  # Manages a single TCP connection with:
  # - Handshake on connect
  # - Length-prefixed frame read/write
  # - Correlation-ID-based request/response multiplexing
  # - A reader thread that dispatches incoming frames
  # - Optional TLS via OpenSSL::SSL
  # - API key authentication via AUTH frame
  #
  # @api private
  class Transport # rubocop:disable Metrics/ClassLength
    HANDSHAKE   = "FIBP\x01\x00".b.freeze
    HEADER_SIZE = 6 # flags:u8 + op:u8 + corr_id:u32

    # Op codes
    OP_ENQUEUE      = 0x01
    OP_CONSUME      = 0x02
    OP_ACK          = 0x03
    OP_NACK         = 0x04
    OP_AUTH         = 0x30
    OP_FLOW         = 0x20
    OP_HEARTBEAT    = 0x21
    OP_ERROR        = 0xFE
    OP_GOAWAY       = 0xFF

    # Frame flags
    FLAG_SERVER_PUSH = 0x04 # bit 2: server-push message frame

    # Error codes returned by the server inside ERROR frames
    ERR_QUEUE_NOT_FOUND    = 1
    ERR_MESSAGE_NOT_FOUND  = 2
    ERR_UNAUTHENTICATED    = 3

    # Sentinel raised when the connection is closed.
    class ConnectionClosed < StandardError; end

    # @param host [String]
    # @param port [Integer]
    # @param tls [Boolean]
    # @param ca_cert [String, nil] PEM CA cert
    # @param client_cert [String, nil] PEM client cert (mTLS)
    # @param client_key [String, nil] PEM client key (mTLS)
    # @param api_key [String, nil]
    def initialize( # rubocop:disable Metrics/ParameterLists
      host:, port:, tls: false, ca_cert: nil, client_cert: nil, client_key: nil, api_key: nil
    )
      @host        = host
      @port        = port
      @tls         = tls || ca_cert
      @ca_cert     = ca_cert
      @client_cert = client_cert
      @client_key  = client_key
      @api_key     = api_key

      @mutex       = Mutex.new
      @corr_seq    = 0
      @pending     = {}          # corr_id => Queue
      @push_queue  = nil         # set during consume_stream
      @closed      = false

      @socket = connect_socket
      perform_handshake
      start_reader
      send_auth if @api_key
    end

    # Send a request frame and block until the response arrives.
    #
    # @param opcode [Integer] op code
    # @param payload [String] binary payload (encoding: BINARY)
    # @return [String] response payload
    # @raise [Fila::QueueNotFoundError, Fila::MessageNotFoundError, Fila::RPCError, ConnectionClosed]
    def request(opcode, payload)
      corr_id = next_corr_id
      result_q = Queue.new

      @mutex.synchronize do
        raise ConnectionClosed, 'connection is closed' if @closed

        @pending[corr_id] = result_q
      end

      write_frame(opcode, corr_id, payload)

      outcome = result_q.pop
      case outcome
      when String    then outcome
      when Exception then raise outcome
      else raise RPCError.new(0, "unexpected transport result: #{outcome.inspect}")
      end
    end

    # Register a push queue for consume-stream server-push frames.
    # Returns corr_id used to issue the consume request.
    #
    # The server sends two kinds of frames after a consume request:
    #   1. An ACK frame (flags=0, corr_id=<request corr_id>, empty payload)
    #      confirming the consume subscription was registered.
    #   2. Push frames (FLAG_SERVER_PUSH set, corr_id=0) carrying messages.
    #
    # We use a one-shot queue for the ACK (to block until the subscription is
    # confirmed) and register +push_q+ at corr_id=0 for ongoing push frames.
    #
    # @param payload [String] consume request payload
    # @param push_q [Queue] messages pushed here as they arrive
    # @return [Integer] corr_id
    def start_consume(payload, push_q)
      corr_id = next_corr_id
      ack_q   = Queue.new

      @mutex.synchronize do
        raise ConnectionClosed, 'connection is closed' if @closed

        @pending[corr_id] = ack_q  # consume ACK routed here (one-shot)
        @pending[0]       = push_q # server-push frames carry corr_id=0
      end

      write_frame(OP_CONSUME, corr_id, payload)

      # Wait for the ACK so we know the subscription is active before returning.
      outcome = ack_q.pop
      raise outcome if outcome.is_a?(Exception)

      corr_id
    end

    # Remove the consume push queue and stop dispatching to it.
    def stop_consume(corr_id)
      @mutex.synchronize do
        @pending.delete(corr_id)
        @pending.delete(0)
      end
    end

    # Close the connection.
    def close
      @mutex.synchronize { @closed = true }
      begin
        @socket.close
      rescue IOError, OpenSSL::SSL::SSLError
        nil
      end
      @reader_thread&.join(2)
      drain_pending
    end

    private

    def connect_socket
      raw = TCPSocket.new(@host, @port)
      raw.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

      return raw unless @tls

      ctx = OpenSSL::SSL::SSLContext.new
      ctx.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER)

      if @ca_cert
        store = OpenSSL::X509::Store.new
        store.add_cert(OpenSSL::X509::Certificate.new(@ca_cert))
        ctx.cert_store = store
      end
      # else: use default system trust store

      if @client_cert && @client_key
        ctx.cert = OpenSSL::X509::Certificate.new(@client_cert)
        ctx.key  = OpenSSL::PKey::RSA.new(@client_key)
      end

      ssl = OpenSSL::SSL::SSLSocket.new(raw, ctx)
      ssl.hostname = @host
      ssl.connect
      ssl
    end

    def perform_handshake
      write_raw(HANDSHAKE)
      echo = read_raw(6)
      raise RPCError.new(0, "FIBP handshake failed: got #{echo.inspect}") unless echo == HANDSHAKE
    end

    def send_auth
      # The FIBP AUTH frame payload is the raw API key bytes — no length prefix.
      payload = @api_key.encode('UTF-8').b
      request(OP_AUTH, payload)
    end

    def start_reader
      @reader_thread = Thread.new { reader_loop }
      @reader_thread.abort_on_exception = false
    end

    def reader_loop
      loop do
        # Read 4-byte length prefix
        len_bytes = read_raw(4)
        break unless len_bytes

        total_len = len_bytes.unpack1('N')
        frame = read_raw(total_len)
        break unless frame && frame.bytesize == total_len

        dispatch_frame(frame)
      end
    rescue IOError, Errno::ECONNRESET, Errno::EPIPE, OpenSSL::SSL::SSLError
      # connection dropped
    ensure
      @mutex.synchronize { @closed = true }
      drain_pending
    end

    def dispatch_frame(frame)
      flags   = frame.getbyte(0)
      opcode  = frame.getbyte(1)
      corr_id = frame.byteslice(2, 4).unpack1('N')
      payload = frame.byteslice(HEADER_SIZE, frame.bytesize - HEADER_SIZE) || ''.b

      dest = @mutex.synchronize { @pending[corr_id] }
      return unless dest

      return drain_pending if opcode == OP_GOAWAY

      push_only = flags.anybits?(FLAG_SERVER_PUSH)
      result    = opcode == OP_ERROR ? parse_error_frame(payload) : payload

      @mutex.synchronize { @pending.delete(corr_id) } unless push_only
      dest.push(result)
    end

    # Parse an OP_ERROR frame payload.
    #
    # FIBP OP_ERROR frames carry a plain UTF-8 message (no numeric code
    # prefix).  We map well-known error messages to typed Ruby exceptions so
    # callers can rescue specific error classes.
    def parse_error_frame(payload)
      msg = payload.force_encoding('UTF-8')
      error_from_message(msg)
    end

    # Map a plain-text FIBP error message to the appropriate Ruby exception.
    def error_from_message(msg)
      return RPCError.new(ERR_UNAUTHENTICATED, msg) if auth_error?(msg)
      return QueueNotFoundError.new(msg)            if queue_not_found_error?(msg)
      return MessageNotFoundError.new(msg)          if message_not_found_error?(msg)

      RPCError.new(0, msg)
    end

    def auth_error?(msg)
      msg.include?('authentication') || msg.include?('unauthenticated') ||
        msg.include?('api key') || msg.include?('OP_AUTH')
    end

    def queue_not_found_error?(msg)
      msg.include?('queue not found') || msg.include?('queue does not exist')
    end

    def message_not_found_error?(msg)
      msg.include?('message not found') || msg.include?('lease not found')
    end

    def write_frame(opcode, corr_id, payload)
      flags  = 0
      header = [flags, opcode, corr_id].pack('CCN')
      body   = header + payload.b
      write_raw([body.bytesize].pack('N') + body)
    end

    def write_raw(bytes)
      @mutex.synchronize do
        @socket.write(bytes)
      end
    rescue IOError, Errno::EPIPE, Errno::ECONNRESET, OpenSSL::SSL::SSLError => e
      raise ConnectionClosed, "write failed: #{e.message}"
    end

    def read_raw(num_bytes)
      buf = ''.b
      while buf.bytesize < num_bytes
        chunk = @socket.read(num_bytes - buf.bytesize)
        return nil if chunk.nil? || chunk.empty?

        buf << chunk
      end
      buf
    end

    # corr_id=0 is permanently reserved for server-push frames.  Regular
    # request IDs cycle through 1..0xFFFFFFFF so they never collide.
    def next_corr_id
      @mutex.synchronize do
        @corr_seq += 1
        @corr_seq = 1 if @corr_seq > 0xFFFFFFFF
        @corr_seq
      end
    end

    def drain_pending
      err = ConnectionClosed.new('connection closed')
      @mutex.synchronize do
        @pending.each_value do |queue|
          queue.push(err)
        rescue StandardError
          nil
        end
        @pending.clear
      end
    end
  end
end
