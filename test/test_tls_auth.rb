# frozen_string_literal: true

require 'test_helper'
require 'openssl'

return unless FILA_SERVER_AVAILABLE

# Helper to generate self-signed CA and server/client certificates for tests.
module CertHelper
  def self.generate_certs(dir)
    # CA key + self-signed cert.
    ca_key = OpenSSL::PKey::RSA.new(2048)
    ca_cert = OpenSSL::X509::Certificate.new
    ca_cert.version = 2
    ca_cert.serial = 1
    ca_cert.subject = OpenSSL::X509::Name.parse('/CN=fila-test-ca')
    ca_cert.issuer = ca_cert.subject
    ca_cert.public_key = ca_key.public_key
    ca_cert.not_before = Time.now - 60
    ca_cert.not_after = Time.now + 3600

    ef = OpenSSL::X509::ExtensionFactory.new
    ef.subject_certificate = ca_cert
    ef.issuer_certificate = ca_cert
    ca_cert.add_extension(ef.create_extension('basicConstraints', 'CA:TRUE', true))
    ca_cert.add_extension(ef.create_extension('keyUsage', 'keyCertSign,cRLSign', true))
    ca_cert.sign(ca_key, OpenSSL::Digest.new('SHA256'))

    # Server key + cert signed by CA.
    server_key = OpenSSL::PKey::RSA.new(2048)
    server_cert = issue_cert(
      ca_cert, ca_key, server_key, '/CN=localhost', 2,
      [['subjectAltName', 'DNS:localhost,IP:127.0.0.1']]
    )

    # Client key + cert signed by CA (for mTLS).
    client_key = OpenSSL::PKey::RSA.new(2048)
    client_cert = issue_cert(ca_cert, ca_key, client_key, '/CN=fila-test-client', 3)

    paths = {}
    { ca_cert: ca_cert, ca_key: ca_key,
      server_cert: server_cert, server_key: server_key,
      client_cert: client_cert, client_key: client_key }.each do |name, obj|
      path = File.join(dir, "#{name}.pem")
      File.write(path, obj.to_pem)
      paths[name] = path
    end
    paths
  end

  def self.issue_cert(ca_cert, ca_key, key, subject_cn, serial, san_entries = [])
    cert = OpenSSL::X509::Certificate.new
    cert.version = 2
    cert.serial = serial
    cert.subject = OpenSSL::X509::Name.parse(subject_cn)
    cert.issuer = ca_cert.subject
    cert.public_key = key.public_key
    cert.not_before = Time.now - 60
    cert.not_after = Time.now + 3600

    ef = OpenSSL::X509::ExtensionFactory.new
    ef.subject_certificate = cert
    ef.issuer_certificate = ca_cert

    san_entries.each do |name, value|
      cert.add_extension(ef.create_extension(name, value, false))
    end

    cert.sign(ca_key, OpenSSL::Digest.new('SHA256'))
    cert
  end
end

class TestApiKeyAuth < Minitest::Test
  def setup
    @bootstrap_key = 'test-bootstrap-key-12345'
    @server = TestServerHelper.start(bootstrap_apikey: @bootstrap_key)
    @client = Fila::Client.new(@server[:addr], api_key: @bootstrap_key)
  end

  def teardown
    @client&.close
    TestServerHelper.stop(@server) if @server
  end

  def test_enqueue_with_api_key
    TestServerHelper.create_queue(@server, 'auth-test-queue')

    msg_id = @client.enqueue(queue: 'auth-test-queue', payload: 'authenticated message')
    assert msg_id
    refute_empty msg_id
  end

  def test_enqueue_without_api_key_rejected
    client_no_key = Fila::Client.new(@server[:addr])
    TestServerHelper.create_queue(@server, 'auth-reject-queue')

    # Without API key, the server should reject with Unauthenticated.
    err = assert_raises(Fila::RPCError) do
      client_no_key.enqueue(queue: 'auth-reject-queue', payload: 'should fail')
    end
    assert_equal Fila::Transport::ERR_UNAUTHENTICATED, err.code
  ensure
    client_no_key&.close
  end

  def test_consume_with_api_key
    TestServerHelper.create_queue(@server, 'auth-consume-queue')
    @client.enqueue(queue: 'auth-consume-queue', payload: 'auth-msg')

    received = false
    @client.consume(queue: 'auth-consume-queue') do |msg|
      assert_equal 'auth-msg', msg.payload
      @client.ack(queue: 'auth-consume-queue', msg_id: msg.id)
      received = true
      break
    end
    assert received
  end
end

class TestTlsConnection < Minitest::Test
  def setup
    @cert_dir = Dir.mktmpdir('fila-certs-')
    @certs = CertHelper.generate_certs(@cert_dir)

    # Server-only TLS: omit ca_cert_path so server does not require client certs.
    # client_ca_cert_path is used by the test client to verify the server cert.
    @server = TestServerHelper.start(
      tls_config: {
        server_cert_path: @certs[:server_cert],
        server_key_path: @certs[:server_key],
        client_ca_cert_path: @certs[:ca_cert]
      }
    )

    @client = Fila::Client.new(
      @server[:addr],
      ca_cert: File.read(@certs[:ca_cert])
    )
  end

  def teardown
    @client&.close
    TestServerHelper.stop(@server) if @server
    FileUtils.rm_rf(@cert_dir) if @cert_dir
  end

  def test_enqueue_over_tls
    TestServerHelper.create_queue(@server, 'tls-test-queue')

    msg_id = @client.enqueue(queue: 'tls-test-queue', payload: 'tls message')
    assert msg_id
    refute_empty msg_id
  end

  def test_consume_over_tls
    TestServerHelper.create_queue(@server, 'tls-consume-queue')
    @client.enqueue(queue: 'tls-consume-queue', payload: 'tls-msg')

    received = false
    @client.consume(queue: 'tls-consume-queue') do |msg|
      assert_equal 'tls-msg', msg.payload
      @client.ack(queue: 'tls-consume-queue', msg_id: msg.id)
      received = true
      break
    end
    assert received
  end
end

class TestMtlsConnection < Minitest::Test
  def setup
    @cert_dir = Dir.mktmpdir('fila-certs-')
    @certs = CertHelper.generate_certs(@cert_dir)

    @server = TestServerHelper.start(
      tls_config: {
        ca_cert_path: @certs[:ca_cert],
        server_cert_path: @certs[:server_cert],
        server_key_path: @certs[:server_key],
        client_cert_path: @certs[:client_cert],
        client_key_path: @certs[:client_key]
      }
    )

    @client = Fila::Client.new(
      @server[:addr],
      ca_cert: File.read(@certs[:ca_cert]),
      client_cert: File.read(@certs[:client_cert]),
      client_key: File.read(@certs[:client_key])
    )
  end

  def teardown
    @client&.close
    TestServerHelper.stop(@server) if @server
    FileUtils.rm_rf(@cert_dir) if @cert_dir
  end

  def test_enqueue_over_mtls
    TestServerHelper.create_queue(@server, 'mtls-test-queue')

    msg_id = @client.enqueue(queue: 'mtls-test-queue', payload: 'mtls message')
    assert msg_id
    refute_empty msg_id
  end
end

class TestTlsWithApiKey < Minitest::Test
  def setup
    @cert_dir = Dir.mktmpdir('fila-certs-')
    @certs = CertHelper.generate_certs(@cert_dir)
    @bootstrap_key = 'tls-bootstrap-key-67890'

    @server = TestServerHelper.start(
      tls_config: {
        server_cert_path: @certs[:server_cert],
        server_key_path: @certs[:server_key],
        client_ca_cert_path: @certs[:ca_cert]
      },
      bootstrap_apikey: @bootstrap_key
    )

    @client = Fila::Client.new(
      @server[:addr],
      ca_cert: File.read(@certs[:ca_cert]),
      api_key: @bootstrap_key
    )
  end

  def teardown
    @client&.close
    TestServerHelper.stop(@server) if @server
    FileUtils.rm_rf(@cert_dir) if @cert_dir
  end

  def test_enqueue_with_tls_and_api_key
    TestServerHelper.create_queue(@server, 'tls-auth-queue')

    msg_id = @client.enqueue(queue: 'tls-auth-queue', payload: 'secure message')
    assert msg_id
    refute_empty msg_id
  end

  def test_no_api_key_over_tls_rejected
    client_no_key = Fila::Client.new(
      @server[:addr],
      ca_cert: File.read(@certs[:ca_cert])
    )
    TestServerHelper.create_queue(@server, 'tls-auth-reject-queue')

    err = assert_raises(Fila::RPCError) do
      client_no_key.enqueue(queue: 'tls-auth-reject-queue', payload: 'should fail')
    end
    assert_equal Fila::Transport::ERR_UNAUTHENTICATED, err.code
  ensure
    client_no_key&.close
  end
end

class TestBackwardCompatibility < Minitest::Test
  def setup
    @server = TestServerHelper.start
    @client = Fila::Client.new(@server[:addr])
  end

  def teardown
    @client&.close
    TestServerHelper.stop(@server) if @server
  end

  def test_plaintext_no_auth_still_works
    TestServerHelper.create_queue(@server, 'compat-queue')

    msg_id = @client.enqueue(queue: 'compat-queue', payload: 'backward compat')
    assert msg_id
    refute_empty msg_id

    received = false
    @client.consume(queue: 'compat-queue') do |msg|
      assert_equal 'backward compat', msg.payload
      @client.ack(queue: 'compat-queue', msg_id: msg.id)
      received = true
      break
    end
    assert received
  end
end
