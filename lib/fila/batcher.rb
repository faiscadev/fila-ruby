# frozen_string_literal: true

module Fila
  # Background batcher that collects enqueue messages and flushes them
  # in batches via the FIBP transport. Supports auto (opportunistic)
  # and linger (timer-based) modes.
  #
  # @api private
  class Batcher # rubocop:disable Metrics/ClassLength
    # An item queued for batching, pairing a message with its result slot.
    BatchItem = Struct.new(:message, :result_queue, keyword_init: true)

    # @param transport [Fila::Transport] FIBP transport
    # @param mode [Symbol] :auto or :linger
    # @param max_batch_size [Integer] cap on batch size (auto mode)
    # @param batch_size [Integer] batch size threshold (linger mode)
    # @param linger_ms [Integer] linger time in ms (linger mode)
    def initialize(transport:, mode:, max_batch_size: 100, batch_size: 100, linger_ms: 10)
      @transport     = transport
      @mode          = mode
      @max_batch_size = mode == :auto ? max_batch_size : batch_size
      @linger_ms     = linger_ms
      @queue         = Queue.new
      @stopped       = false
      @mutex         = Mutex.new

      @thread = Thread.new { run_loop }
      @thread.abort_on_exception = true
    end

    # Submit a message for batched sending. Blocks until the batch
    # containing this message is flushed and the result is available.
    #
    # @param message [Hash] enqueue message with :queue, :payload, :headers
    # @return [String] message ID on success
    # @raise [Fila::QueueNotFoundError] if the queue does not exist
    # @raise [Fila::RPCError] for unexpected transport failures
    def submit(message)
      result_queue = Queue.new
      item = BatchItem.new(message: message, result_queue: result_queue)

      @mutex.synchronize do
        raise Fila::Error, 'batcher is closed' if @stopped

        @queue.push(item)
      end

      outcome = result_queue.pop
      case outcome
      when String    then outcome
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
      when :auto   then run_auto_loop
      when :linger then run_linger_loop
      end
    end

    # Auto mode: block for the first message, then non-blocking drain
    # any additional messages that have arrived, flush concurrently.
    def run_auto_loop
      loop do
        first = @queue.pop
        break if first == :shutdown

        batch = [first]
        drain_nonblocking(batch)
        flush_batch(batch)
      end
    end

    # Linger mode: block for the first message, then wait up to linger_ms
    # for more messages or until batch_size is reached.
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

    # Flush a batch of items via the FIBP transport.
    # Groups items by queue to produce one frame per queue.
    def flush_batch(items) # rubocop:disable Metrics/MethodLength
      # Group by queue, preserving per-item result queues
      groups = items.each_with_index.group_by { |item, _| item.message[:queue] }

      groups.each do |queue, indexed_items|
        items_only  = indexed_items.map(&:first)
        msgs        = items_only.map(&:message)
        payload     = Codec.encode_enqueue(queue, msgs)
        resp        = @transport.request(Transport::OP_ENQUEUE, payload)
        results     = Codec.decode_enqueue_response(resp)

        items_only.each_with_index do |item, i|
          item.result_queue.push(result_to_outcome(results[i]))
        end
      end
    rescue Transport::ConnectionClosed => e
      broadcast_error(items, RPCError.new(0, "connection closed: #{e.message}"))
    rescue StandardError => e
      broadcast_error(items, Fila::Error.new(e.message))
    end

    # Convert an EnqueueResult into a String (message_id) or Exception.
    def result_to_outcome(result)
      return Fila::Error.new('no result from server') if result.nil?
      return result.message_id if result.success?

      QueueNotFoundError.new("enqueue: #{result.error}")
    end

    def broadcast_error(items, err)
      items.each { |item| item.result_queue.push(err) rescue nil } # rubocop:disable Style/RescueModifier
    end

    def current_time_ms
      (Process.clock_gettime(Process::CLOCK_MONOTONIC) * 1000).to_i
    end

    # Pop from @queue with a timeout in milliseconds.
    # Raises ThreadError if nothing is available within the timeout.
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
