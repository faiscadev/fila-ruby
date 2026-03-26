# frozen_string_literal: true

require 'minitest/autorun'
require 'tmpdir'
require 'socket'
require 'openssl'

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
require 'fila'

FILA_SERVER_BIN = ENV.fetch('FILA_SERVER_BIN') do
  File.join(__dir__, '..', '..', 'fila', 'target', 'release', 'fila-server')
end
FILA_SERVER_AVAILABLE = File.exist?(FILA_SERVER_BIN)

module TestServerHelper # rubocop:disable Metrics/ModuleLength
  def self.find_free_port
    server = TCPServer.new('127.0.0.1', 0)
    port = server.addr[1]
    server.close
    port
  end

  # Start a fila-server instance.
  #
  # @param tls_config [Hash, nil] optional TLS configuration with keys:
  #   :ca_cert_path, :server_cert_path, :server_key_path
  # @param bootstrap_apikey [String, nil] optional bootstrap API key
  # @return [Hash] server info with :addr, :host, :port, :pid, :data_dir
  #   and optional :tls_config, :bootstrap_apikey
  def self.start(tls_config: nil, bootstrap_apikey: nil)
    # Retry up to 3 times to handle the TOCTOU race between find_free_port
    # and the server process binding the port.
    3.times do |attempt|
      result = try_start(tls_config: tls_config, bootstrap_apikey: bootstrap_apikey)
      return result if result

      raise "fila-server failed to bind after #{attempt + 1} attempt(s)" if attempt == 2
    end
  end

  # Attempt to start a fila-server instance once. Returns the server info
  # hash on success, or nil if the port was already in use (retry-able).
  # Raises on any other failure.
  def self.try_start(tls_config: nil, bootstrap_apikey: nil)
    port = find_free_port
    addr = "127.0.0.1:#{port}"

    data_dir    = Dir.mktmpdir('fila-test-')
    config_path = File.join(data_dir, 'fila.toml')

    toml = "[fibp]\nlisten_addr = \"#{addr}\"\n"

    if tls_config
      toml += "\n[tls]\n"
      toml += "cert_file = \"#{tls_config[:server_cert_path]}\"\n"
      toml += "key_file = \"#{tls_config[:server_key_path]}\"\n"
      toml += "ca_file = \"#{tls_config[:ca_cert_path]}\"\n" if tls_config[:ca_cert_path]
    end

    File.write(config_path, toml)
    db_dir = File.join(data_dir, 'db')

    env = { 'FILA_DATA_DIR' => db_dir }
    env['FILA_BOOTSTRAP_APIKEY'] = bootstrap_apikey if bootstrap_apikey

    stderr_path = File.join(data_dir, 'stderr.log')
    pid = File.open(stderr_path, 'w') do |stderr_file|
      Process.spawn(
        env,
        FILA_SERVER_BIN,
        chdir: data_dir,
        out: File::NULL,
        err: stderr_file
      )
    end

    server_info = {
      addr: addr,
      host: '127.0.0.1',
      port: port,
      pid: pid,
      data_dir: data_dir,
      tls_config: tls_config,
      bootstrap_apikey: bootstrap_apikey
    }

    wait_for_ready(server_info, stderr_path, toml)
    server_info
  rescue RuntimeError => e
    # If the port was already in use the server exits immediately and stderr
    # contains "Address already in use".  Clean up and signal the caller to
    # retry with a different port.
    if e.message.include?('Address already in use')
      FileUtils.rm_rf(data_dir)
      return nil
    end

    raise
  end

  FIBP_HANDSHAKE = "FIBP\x01\x00".b.freeze

  def self.wait_for_ready(server_info, stderr_path, toml)
    deadline = Time.now + 10
    ready = false
    while Time.now < deadline
      begin
        # Perform a full FIBP handshake probe so we only return when the server
        # is actually ready to accept FIBP connections, not just when the TCP
        # port is open.  A plain TCP connect can succeed before the server has
        # finished its initialization, causing the first real request to fail.
        fibp_ready = probe_fibp(server_info[:host], server_info[:port],
                                server_info[:tls_config])
        if fibp_ready
          ready = true
          break
        end
      rescue SystemCallError, IOError, OpenSSL::SSL::SSLError
        # not up yet — fall through to sleep
      end
      sleep 0.05
    end

    return if ready

    Process.kill('TERM', server_info[:pid])
    Process.wait(server_info[:pid])
    stderr_output = begin
      File.read(stderr_path)
    rescue StandardError
      ''
    end
    FileUtils.rm_rf(server_info[:data_dir])
    raise "fila-server failed to start within 10s on #{server_info[:addr]}\nConfig:\n#{toml}\nStderr:\n#{stderr_output}"
  end

  # Attempt a single FIBP handshake probe. Returns true if the server echoes
  # the 6-byte handshake back within the timeout.  Raises SystemCallError if
  # the port is not yet accepting connections.
  #
  # Uses a blocking read with IO.select so a single probe attempt waits up to
  # PROBE_TIMEOUT_S seconds rather than returning false immediately and forcing
  # the caller to reconnect (which can leave half-open connections).
  PROBE_TIMEOUT_S = 0.5

  def self.probe_fibp(host, port, tls_config)
    tcp = TCPSocket.new(host, port)
    tcp.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
    sock = tcp

    if tls_config
      ctx = OpenSSL::SSL::SSLContext.new
      # Don't verify the server cert in the readiness probe — we only need to
      # confirm the server is accepting FIBP connections.
      ctx.set_params(verify_mode: OpenSSL::SSL::VERIFY_NONE)

      # For mTLS servers the server requires a client cert; supply one when
      # available so the TLS handshake succeeds.
      if tls_config[:client_cert_path] && tls_config[:client_key_path]
        ctx.cert = OpenSSL::X509::Certificate.new(File.read(tls_config[:client_cert_path]))
        ctx.key  = OpenSSL::PKey::RSA.new(File.read(tls_config[:client_key_path]))
      end

      ssl = OpenSSL::SSL::SSLSocket.new(tcp, ctx)
      ssl.hostname = host
      ssl.connect
      sock = ssl
    end

    sock.write(FIBP_HANDSHAKE)

    # Wait up to PROBE_TIMEOUT_S for the server to echo back the handshake.
    return false unless sock.wait_readable(PROBE_TIMEOUT_S)

    echo = sock.read(6)
    echo == FIBP_HANDSHAKE
  ensure
    sock&.close rescue nil # rubocop:disable Style/RescueModifier
    tcp.close rescue nil if sock != tcp # rubocop:disable Style/RescueModifier
  end

  def self.stop(server)
    Process.kill('TERM', server[:pid])
    Process.wait(server[:pid])
    FileUtils.rm_rf(server[:data_dir])
  rescue Errno::ESRCH, Errno::ECHILD
    # Process already gone.
  end

  # Build a FIBP transport with appropriate TLS/auth for admin operations.
  def self.admin_transport(server)
    tc = server[:tls_config]
    tls_opts = if tc
                 ca_path = tc[:client_ca_cert_path] || tc[:ca_cert_path]
                 {
                   tls: true,
                   ca_cert: ca_path ? File.read(ca_path) : nil,
                   client_cert: tc[:client_cert_path] ? File.read(tc[:client_cert_path]) : nil,
                   client_key: tc[:client_key_path] ? File.read(tc[:client_key_path]) : nil
                 }
               else
                 { tls: false }
               end

    Fila::Transport.new(
      host: server[:host],
      port: server[:port],
      api_key: server[:bootstrap_apikey],
      **tls_opts
    )
  end

  # Send a CreateQueue admin frame via FIBP.
  # The payload is a protobuf-encoded CreateQueueRequest { name: <string> }.
  OP_CREATE_QUEUE = 0x10

  def self.create_queue(server, name)
    transport = admin_transport(server)
    payload   = proto_encode_create_queue(name)
    transport.request(OP_CREATE_QUEUE, payload)
  rescue StandardError => e
    raise "create_queue #{name.inspect} failed: #{e.message}"
  ensure
    transport&.close
  end

  # Hand-encode a CreateQueueRequest protobuf message.
  #
  # CreateQueueRequest { string name = 1; QueueConfig config = 2; }
  #
  # We only set field 1 (name).  For strings ≤ 127 bytes the varint length
  # fits in one byte, which covers all queue names used in tests.
  #
  # Proto3 wire format for a string field:
  #   tag:   (field_number << 3) | wire_type  → field 1, wire type 2 → 0x0a
  #   len:   varint-encoded byte length of the string
  #   data:  UTF-8 bytes
  def self.proto_encode_create_queue(name)
    name_b = name.encode('UTF-8').b
    raise ArgumentError, "queue name too long (#{name_b.bytesize} bytes)" if name_b.bytesize > 127

    "\x0a".b + [name_b.bytesize].pack('C') + name_b
  end
end
