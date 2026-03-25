# frozen_string_literal: true

require 'test_helper'

return unless FILA_SERVER_AVAILABLE

class TestEnqueueMany < Minitest::Test
  def setup
    @server = TestServerHelper.start
    @client = Fila::Client.new(@server[:addr], batch_mode: :disabled)
  end

  def teardown
    @client&.close
    TestServerHelper.stop(@server) if @server
  end

  def test_enqueue_many_multiple_messages
    TestServerHelper.create_queue(@server, 'many-test')

    messages = 5.times.map do |i|
      { queue: 'many-test', payload: "many-msg-#{i}", headers: { 'index' => i.to_s } }
    end

    results = @client.enqueue_many(messages)

    assert_equal 5, results.size
    results.each do |r|
      assert r.success?, "expected success but got error: #{r.error}"
      refute_empty r.message_id
    end

    # Verify all messages are consumable.
    received_ids = []
    @client.consume(queue: 'many-test') do |msg|
      received_ids << msg.id
      @client.ack(queue: 'many-test', msg_id: msg.id)
      break if received_ids.size >= 5
    end
    assert_equal 5, received_ids.size
  end

  def test_enqueue_many_single_message
    TestServerHelper.create_queue(@server, 'many-single')

    results = @client.enqueue_many(
      [{ queue: 'many-single', payload: 'solo' }]
    )

    assert_equal 1, results.size
    assert results.first.success?
    refute_empty results.first.message_id
  end

  def test_enqueue_many_empty_array
    results = @client.enqueue_many([])
    assert_equal 0, results.size
  end

  def test_enqueue_many_mixed_success_and_failure
    TestServerHelper.create_queue(@server, 'many-mixed')

    messages = [
      { queue: 'many-mixed', payload: 'good-1' },
      { queue: 'no-such-queue-xyz', payload: 'bad' },
      { queue: 'many-mixed', payload: 'good-2' }
    ]

    results = @client.enqueue_many(messages)
    assert_equal 3, results.size

    assert results[0].success?, 'first message should succeed'
    refute results[1].success?, 'second message should fail (nonexistent queue)'
    assert results[1].error, 'second message should have error description'
    assert results[2].success?, 'third message should succeed'
  end
end

class TestEnqueueResult < Minitest::Test
  def test_success_result
    r = Fila::EnqueueResult.new(message_id: 'abc-123')
    assert r.success?
    assert_equal 'abc-123', r.message_id
    assert_nil r.error
  end

  def test_error_result
    r = Fila::EnqueueResult.new(error: 'queue not found')
    refute r.success?
    assert_nil r.message_id
    assert_equal 'queue not found', r.error
  end
end

class TestAutoBatching < Minitest::Test
  def setup
    @server = TestServerHelper.start
    # Default batch_mode is :auto
    @client = Fila::Client.new(@server[:addr])
  end

  def teardown
    @client&.close
    TestServerHelper.stop(@server) if @server
  end

  def test_auto_batch_enqueue_single
    TestServerHelper.create_queue(@server, 'auto-single')

    msg_id = @client.enqueue(queue: 'auto-single', payload: 'auto-msg')
    assert msg_id
    refute_empty msg_id

    received = false
    @client.consume(queue: 'auto-single') do |msg|
      assert_equal msg_id, msg.id
      assert_equal 'auto-msg', msg.payload
      @client.ack(queue: 'auto-single', msg_id: msg.id)
      received = true
      break
    end
    assert received
  end

  def test_auto_batch_enqueue_concurrent
    TestServerHelper.create_queue(@server, 'auto-concurrent')

    # Fire multiple enqueues concurrently to exercise batching.
    threads = 10.times.map do |i|
      Thread.new do
        @client.enqueue(queue: 'auto-concurrent', payload: "msg-#{i}")
      end
    end
    ids = threads.map(&:value)

    assert_equal 10, ids.size
    ids.each do |id|
      assert id
      refute_empty id
    end

    # Consume all messages.
    received = []
    @client.consume(queue: 'auto-concurrent') do |msg|
      received << msg.id
      @client.ack(queue: 'auto-concurrent', msg_id: msg.id)
      break if received.size >= 10
    end
    assert_equal 10, received.size
  end

  def test_auto_batch_nonexistent_queue_raises
    assert_raises(Fila::QueueNotFoundError) do
      @client.enqueue(queue: 'no-such-queue-auto', payload: 'fail')
    end
  end
end

class TestLingerBatching < Minitest::Test
  def setup
    @server = TestServerHelper.start
    @client = Fila::Client.new(@server[:addr], batch_mode: :linger, linger_ms: 50, batch_size: 10)
  end

  def teardown
    @client&.close
    TestServerHelper.stop(@server) if @server
  end

  def test_linger_batch_enqueue
    TestServerHelper.create_queue(@server, 'linger-test')

    msg_id = @client.enqueue(queue: 'linger-test', payload: 'linger-msg')
    assert msg_id
    refute_empty msg_id
  end

  def test_linger_batch_concurrent
    TestServerHelper.create_queue(@server, 'linger-concurrent')

    threads = 5.times.map do |i|
      Thread.new do
        @client.enqueue(queue: 'linger-concurrent', payload: "linger-#{i}")
      end
    end
    ids = threads.map(&:value)

    assert_equal 5, ids.size
    ids.each { |id| refute_empty id }
  end
end

class TestDisabledBatching < Minitest::Test
  def setup
    @server = TestServerHelper.start
    @client = Fila::Client.new(@server[:addr], batch_mode: :disabled)
  end

  def teardown
    @client&.close
    TestServerHelper.stop(@server) if @server
  end

  def test_disabled_batch_enqueue_direct
    TestServerHelper.create_queue(@server, 'disabled-test')

    msg_id = @client.enqueue(queue: 'disabled-test', payload: 'direct-msg')
    assert msg_id
    refute_empty msg_id
  end

  def test_disabled_nonexistent_queue_raises
    assert_raises(Fila::QueueNotFoundError) do
      @client.enqueue(queue: 'no-such-queue-disabled', payload: 'fail')
    end
  end
end

class TestBatchModeValidation < Minitest::Test
  def test_invalid_batch_mode_raises
    assert_raises(ArgumentError) do
      Fila::Client.new('localhost:5555', batch_mode: :invalid)
    end
  end

  def test_valid_batch_modes_accepted
    # These should not raise (but won't connect since server isn't on this port).
    # Just verify argument validation passes.
    %i[auto linger disabled].each do |mode|
      client = Fila::Client.new('localhost:19999', batch_mode: mode)
      client.close
    end
  end
end

class TestCloseFlush < Minitest::Test
  def setup
    @server = TestServerHelper.start
  end

  def teardown
    TestServerHelper.stop(@server) if @server
  end

  def test_close_drains_pending_messages
    TestServerHelper.create_queue(@server, 'close-drain')
    client = Fila::Client.new(@server[:addr])

    # Enqueue a message, then close immediately.
    msg_id = client.enqueue(queue: 'close-drain', payload: 'drain-me')
    refute_empty msg_id

    client.close

    # Verify the message was persisted.
    verify_client = Fila::Client.new(@server[:addr], batch_mode: :disabled)
    received = false
    verify_client.consume(queue: 'close-drain') do |msg|
      assert_equal msg_id, msg.id
      verify_client.ack(queue: 'close-drain', msg_id: msg.id)
      received = true
      break
    end
    assert received
    verify_client.close
  end

  def test_double_close_is_safe
    client = Fila::Client.new(@server[:addr])
    client.close
    client.close # Should not raise.
  end
end
