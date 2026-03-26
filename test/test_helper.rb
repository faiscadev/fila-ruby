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

module TestServerHelper
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
  def self.start(tls_config: nil, bootstrap_apikey: nil) # rubocop:disable Metrics/MethodLength
    port = find_free_port
    addr = "127.0.0.1:#{port}"

    data_dir    = Dir.mktmpdir('fila-test-')
    config_path = File.join(data_dir, 'fila.toml')

    toml = "[server]\nlisten_addr = \"#{addr}\"\n"

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
  end

  def self.wait_for_ready(server_info, stderr_path, toml)
    deadline = Time.now + 10
    ready = false
    while Time.now < deadline
      begin
        transport = admin_transport(server_info)
        transport.close
        ready = true
        break
      rescue StandardError
        sleep 0.05
      end
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
  OP_CREATE_QUEUE = 0x10

  def self.create_queue(server, name)
    transport = admin_transport(server)
    name_b    = name.encode('UTF-8').b
    payload   = [name_b.bytesize].pack('n') + name_b +
                [0].pack('n') # config_count: 0 key-value pairs
    transport.request(OP_CREATE_QUEUE, payload)
  rescue StandardError => e
    raise "create_queue #{name.inspect} failed: #{e.message}"
  ensure
    transport&.close
  end
end
