# frozen_string_literal: true

require 'test_helper'

return unless FILA_SERVER_AVAILABLE

class TestClient < Minitest::Test
  def setup
    @server = TestServerHelper.start
    @client = Fila::Client.new(@server[:addr])
  end

  def teardown
    @client&.close
    TestServerHelper.stop(@server) if @server
  end

  def test_enqueue_consume_ack
    TestServerHelper.create_queue(@server, 'test-enqueue-ack')

    msg_id = @client.enqueue(queue: 'test-enqueue-ack', headers: { 'key' => 'value' }, payload: 'hello')
    assert msg_id
    refute_empty msg_id

    received = false
    @client.consume(queue: 'test-enqueue-ack') do |msg|
      assert_equal msg_id, msg.id
      assert_equal({ 'key' => 'value' }, msg.headers)
      assert_equal 'hello', msg.payload
      assert_equal 'test-enqueue-ack', msg.queue
      assert_equal 0, msg.attempt_count

      @client.ack(queue: 'test-enqueue-ack', msg_id: msg.id)
      received = true
      break
    end
    assert received
  end

  def test_nack_redelivers_on_same_stream
    TestServerHelper.create_queue(@server, 'test-nack-redeliver')

    @client.enqueue(queue: 'test-nack-redeliver', payload: 'retry-me')

    delivery_count = 0
    @client.consume(queue: 'test-nack-redeliver') do |msg|
      assert_equal 'retry-me', msg.payload
      if delivery_count.zero?
        assert_equal 0, msg.attempt_count
        @client.nack(queue: 'test-nack-redeliver', msg_id: msg.id, error: 'transient error')
        delivery_count += 1
      else
        assert_equal 1, msg.attempt_count
        @client.ack(queue: 'test-nack-redeliver', msg_id: msg.id)
        break
      end
    end
    assert_equal 1, delivery_count
  end

  def test_enqueue_nonexistent_queue_raises_queue_not_found
    assert_raises(Fila::QueueNotFoundError) do
      @client.enqueue(queue: 'no-such-queue', payload: 'fail')
    end
  end
end
