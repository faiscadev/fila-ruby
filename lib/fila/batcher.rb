# frozen_string_literal: true

module Fila
  # Background batcher that collects enqueue messages and flushes them
  # in batches via the FIBP binary protocol. Supports auto (opportunistic)
  # and linger (timer-based) modes.
  #
  # @api private
  class Batcher # rubocop:disable Metrics/ClassLength
    # An item queued for batching, pairing a message hash with its result slot.
    BatchItem = Struct.new(:message, :result_queue, keyword_init: true)

    # @param conn [Fila::FIBP::Connection] FIBP connection
    # @param mode [Symbol] :auto or :linger
    # @param max_batch_size [Integer] cap on batch size (auto mode)
    # @param batch_size [Integer] batch size threshold (linger mode)
    # @param linger_ms [Integer] linger time in ms (linger mode)
    def initialize(conn:, mode:, max_batch_size: 100, batch_size: 100, linger_ms: 10)
      @conn = conn
      @mode = mode
      @max_batch_size = mode == :auto ? max_batch_size : batch_size
      @linger_ms = linger_ms
      @queue = Queue.new
      @stopped = false
      @mutex = Mutex.new

      @thread = Thread.new { run_loop }
      @thread.abort_on_exception = true
    end

    # Submit a message for batched sending. Blocks until the batch
    # containing this message is flushed and the result is available.
    #
    # @param message [Hash] message hash with :queue, :headers, :payload
    # @return [String] message ID on success
    # @raise [Fila::QueueNotFoundError] if the queue does not exist
    # @raise [Fila::RPCError] for unexpected failures
    def submit(message)
      result_queue = Queue.new
      item = BatchItem.new(message: message, result_queue: result_queue)

      @mutex.synchronize do
        raise Fila::Error, 'batcher is closed' if @stopped

        @queue.push(item)
      end

      outcome = result_queue.pop
      case outcome
      when String then outcome
      when Exception then raise outcome
      else raise Fila::Error, "unexpected batcher result: #{outcome.inspect}"
      end
    end

    # Drain pending messages and stop the background thread.
    def close
      @mutex.synchronize { @stopped = true }
      @queue.push(:shutdown)
      @thread.join
    end

    private

    def run_loop
      case @mode
      when :auto then run_auto_loop
      when :linger then run_linger_loop
      end
    end

    def run_auto_loop
      loop do
        first = @queue.pop
        break if first == :shutdown

        batch = [first]
        drain_nonblocking(batch)
        flush_batch(batch)
      end
    end

    def run_linger_loop
      loop do
        first = @queue.pop
        break if first == :shutdown

        batch = [first]
        deadline = current_time_ms + @linger_ms

        while batch.size < @max_batch_size
          remaining_ms = deadline - current_time_ms
          break if remaining_ms <= 0

          begin
            item = pop_with_timeout(remaining_ms)
            break if item == :shutdown

            batch << item
          rescue ThreadError
            break
          end
        end

        flush_batch(batch)
      end
    end

    def drain_nonblocking(batch)
      while batch.size < @max_batch_size
        begin
          item = @queue.pop(true)
          if item == :shutdown
            @queue.push(:shutdown)
            break
          end
          batch << item
        rescue ThreadError
          break
        end
      end
    end

    # Flush a batch of items via FIBP Enqueue.
    def flush_batch(items)
      messages = items.map(&:message)
      payload = encode_enqueue_batch(messages)
      opcode, resp = @conn.request(FIBP::Opcodes::ENQUEUE, payload)

      if opcode == FIBP::Opcodes::ERROR
        reader = FIBP::Codec::Reader.new(resp)
        code = reader.read_u8
        message = reader.read_string
        broadcast_error(items, RPCError.new(code, message))
        return
      end

      reader = FIBP::Codec::Reader.new(resp)
      count = reader.read_u32

      items.each_with_index do |item, idx|
        if idx < count
          code = reader.read_u8
          msg_id = reader.read_string
          item.result_queue.push(result_to_outcome(code, msg_id))
        else
          item.result_queue.push(Fila::Error.new('no result from server'))
        end
      end
    rescue StandardError => e
      broadcast_error(items, Fila::Error.new(e.message))
    end

    def encode_enqueue_batch(messages)
      buf = FIBP::Codec.encode_u32(messages.size)
      messages.each do |m|
        buf += FIBP::Codec.encode_string(m[:queue]) +
               FIBP::Codec.encode_map(m[:headers] || {}) +
               FIBP::Codec.encode_bytes(m[:payload])
      end
      buf
    end

    def result_to_outcome(code, msg_id)
      case code
      when FIBP::ErrorCodes::OK then msg_id
      when FIBP::ErrorCodes::QUEUE_NOT_FOUND
        QueueNotFoundError.new("enqueue: queue not found")
      else
        RPCError.new(code, "enqueue failed")
      end
    end

    def broadcast_error(items, err)
      items.each { |item| item.result_queue.push(err) }
    end

    def current_time_ms
      (Process.clock_gettime(Process::CLOCK_MONOTONIC) * 1000).to_i
    end

    def pop_with_timeout(timeout_ms)
      deadline = current_time_ms + timeout_ms
      loop do
        return @queue.pop(true)
      rescue ThreadError
        raise if current_time_ms >= deadline

        sleep(0.001)
      end
    end
  end
end
