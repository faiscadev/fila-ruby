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
      send_auth if @api_key
      start_reader
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
    # @param payload [String] consume request payload
    # @param push_q [Queue] messages pushed here as they arrive
    # @return [Integer] corr_id
    def start_consume(payload, push_q)
      corr_id = next_corr_id

      @mutex.synchronize do
        raise ConnectionClosed, 'connection is closed' if @closed

        @pending[corr_id] = push_q
      end

      write_frame(OP_CONSUME, corr_id, payload)
      corr_id
    end

    # Remove the consume push queue and stop dispatching to it.
    def stop_consume(corr_id)
      @mutex.synchronize { @pending.delete(corr_id) }
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
      key_bytes = @api_key.encode('UTF-8').b
      payload = [key_bytes.bytesize].pack('n') + key_bytes
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

    def parse_error_frame(payload)
      err_code = payload.byteslice(0, 2).unpack1('n')
      msg_len  = payload.byteslice(2, 2).unpack1('n')
      msg      = payload.byteslice(4, msg_len).force_encoding('UTF-8')
      case err_code
      when ERR_QUEUE_NOT_FOUND   then QueueNotFoundError.new(msg)
      when ERR_MESSAGE_NOT_FOUND then MessageNotFoundError.new(msg)
      when ERR_UNAUTHENTICATED   then RPCError.new(ERR_UNAUTHENTICATED, msg)
      else                            RPCError.new(err_code, msg)
      end
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

    def next_corr_id
      @mutex.synchronize do
        @corr_seq = (@corr_seq + 1) & 0xFFFFFFFF
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
