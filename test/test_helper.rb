# frozen_string_literal: true

require 'minitest/autorun'
require 'tmpdir'
require 'socket'
require 'grpc'

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
$LOAD_PATH.unshift File.expand_path('../lib/fila/proto', __dir__)
require 'fila'
require_relative '../lib/fila/proto/fila/v1/admin_services_pb'

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
  # @return [Hash] server info with :addr, :pid, :data_dir, :admin_stub
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

    # Build credentials for admin stub.
    # client_ca_cert_path is always needed to verify server cert; ca_cert_path is only for mTLS.
    credentials = :this_channel_is_insecure
    if tls_config
      ca_path = tls_config[:client_ca_cert_path] || tls_config[:ca_cert_path]
      if ca_path
        ca_cert = File.read(ca_path)
        client_key = tls_config[:client_key_path] ? File.read(tls_config[:client_key_path]) : nil
        client_cert = tls_config[:client_cert_path] ? File.read(tls_config[:client_cert_path]) : nil
        credentials = GRPC::Core::ChannelCredentials.new(ca_cert, client_key, client_cert)
      end
    end

    admin_metadata = {}
    admin_metadata['authorization'] = "Bearer #{bootstrap_apikey}" if bootstrap_apikey

    # Wait for server ready.
    deadline = Time.now + 10
    ready = false
    while Time.now < deadline
      begin
        try_list_queues(addr, credentials: credentials, metadata: admin_metadata)
        ready = true
        break
      rescue StandardError
        sleep 0.05
      end
    end

    unless ready
      Process.kill('TERM', pid)
      Process.wait(pid)
      stderr_output = File.read(stderr_path) rescue ''
      FileUtils.rm_rf(data_dir)
      raise "fila-server failed to start within 10s on #{addr}\nConfig:\n#{toml}\nStderr:\n#{stderr_output}"
    end

    admin_stub = ::Fila::V1::FilaAdmin::Stub.new(addr, credentials)

    {
      addr: addr,
      pid: pid,
      data_dir: data_dir,
      admin_stub: admin_stub,
      admin_metadata: admin_metadata
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
    req = ::Fila::V1::CreateQueueRequest.new(name: name, config: {})
    server[:admin_stub].create_queue(req, metadata: server[:admin_metadata] || {})
  end

  def self.try_list_queues(addr, credentials: :this_channel_is_insecure, metadata: {})
    stub = ::Fila::V1::FilaAdmin::Stub.new(addr, credentials)
    stub.list_queues(::Fila::V1::ListQueuesRequest.new, metadata: metadata)
  end
end
