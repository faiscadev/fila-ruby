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

  def self.start
    port = find_free_port
    addr = "127.0.0.1:#{port}"

    data_dir = Dir.mktmpdir('fila-test-')
    config_path = File.join(data_dir, 'fila.toml')
    File.write(config_path, "[server]\nlisten_addr = \"#{addr}\"\n")
    db_dir = File.join(data_dir, 'db')

    pid = Process.spawn(
      { 'FILA_DATA_DIR' => db_dir },
      FILA_SERVER_BIN,
      chdir: data_dir,
      %i[out err] => File::NULL
    )

    # Wait for server ready.
    deadline = Time.now + 10
    ready = false
    while Time.now < deadline
      begin
        try_list_queues(addr)
        ready = true
        break
      rescue StandardError
        sleep 0.05
      end
    end

    unless ready
      Process.kill('TERM', pid)
      Process.wait(pid)
      FileUtils.rm_rf(data_dir)
      raise "fila-server failed to start within 10s on #{addr}"
    end

    admin_stub = ::Fila::V1::FilaAdmin::Stub.new(addr, :this_channel_is_insecure)

    {
      addr: addr,
      pid: pid,
      data_dir: data_dir,
      admin_stub: admin_stub
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
    server[:admin_stub].create_queue(req)
  end

  def self.try_list_queues(addr)
    stub = ::Fila::V1::FilaAdmin::Stub.new(addr, :this_channel_is_insecure)
    stub.list_queues(::Fila::V1::ListQueuesRequest.new)
  end
end
