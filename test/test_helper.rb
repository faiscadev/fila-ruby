# frozen_string_literal: true

require 'minitest/autorun'
require 'tmpdir'
require 'socket'
require 'timeout'

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
  # @param tls_config [Hash, nil] optional TLS configuration
  # @param bootstrap_apikey [String, nil] optional bootstrap API key
  # @return [Hash] server info with :addr, :pid, :data_dir, etc.
  def self.start(tls_config: nil, bootstrap_apikey: nil)
    port = find_free_port
    addr = "127.0.0.1:#{port}"

    data_dir = Dir.mktmpdir('fila-test-')
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

    # Build connection options for admin operations.
    conn_opts = { tls: false }
    if tls_config
      ca_path = tls_config[:client_ca_cert_path] || tls_config[:ca_cert_path]
      if ca_path
        conn_opts[:ca_cert] = File.read(ca_path)
        conn_opts[:client_cert] = File.read(tls_config[:client_cert_path]) if tls_config[:client_cert_path]
        conn_opts[:client_key] = File.read(tls_config[:client_key_path]) if tls_config[:client_key_path]
      end
    end
    conn_opts[:api_key] = bootstrap_apikey if bootstrap_apikey

    # Wait for server ready.
    deadline = Time.now + 10
    ready = false
    last_error = nil
    while Time.now < deadline
      begin
        Timeout.timeout(3) { try_list_queues(addr, conn_opts) }
        ready = true
        break
      rescue StandardError => e
        last_error = e
        sleep 0.1
      end
    end

    unless ready
      Process.kill('TERM', pid)
      Process.wait(pid)
      stderr_output = begin
        File.read(stderr_path)
      rescue StandardError
        ''
      end
      FileUtils.rm_rf(data_dir)
      raise "fila-server failed to start within 10s on #{addr}\nConfig:\n#{toml}\n" \
            "Stderr:\n#{stderr_output}\nLast error: #{last_error&.class}: #{last_error&.message}"
    end

    {
      addr: addr,
      pid: pid,
      data_dir: data_dir,
      conn_opts: conn_opts
    }
  end

  def self.stop(server)
    Process.kill('TERM', server[:pid])
    Process.wait(server[:pid])
    FileUtils.rm_rf(server[:data_dir])
  rescue Errno::ESRCH, Errno::ECHILD
    # Process already gone.
  end

  def self.create_queue(server, name)
    client = nil
    client = Fila::Client.new(server[:addr], batch_mode: :disabled, **server[:conn_opts])
    client.create_queue(name: name)
  ensure
    client&.close
  end

  def self.try_list_queues(addr, conn_opts)
    client = nil
    client = Fila::Client.new(addr, batch_mode: :disabled, **conn_opts)
    client.list_queues
  ensure
    client&.close
  end
end
