# frozen_string_literal: true

module Fila
  # Background batcher that collects enqueue requests and flushes them
  # in batches via BatchEnqueue RPC. Supports auto (opportunistic) and
  # linger (timer-based) modes.
  #
  # @api private
  class Batcher
    # An item queued for batching, pairing a message with its result slot.
    BatchItem = Struct.new(:request, :result_queue, keyword_init: true)

    # @param stub [Fila::V1::FilaService::Stub] gRPC stub
    # @param metadata [Hash] call metadata (auth headers)
    # @param mode [Symbol] :auto or :linger
    # @param max_batch_size [Integer] cap on batch size (auto mode)
    # @param batch_size [Integer] batch size threshold (linger mode)
    # @param linger_ms [Integer] linger time in ms (linger mode)
    def initialize(stub:, metadata:, mode:, max_batch_size: 100, batch_size: 100, linger_ms: 10)
      @stub = stub
      @metadata = metadata
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
    # @param request [Fila::V1::EnqueueRequest] the enqueue request
    # @return [String] message ID on success
    # @raise [Fila::QueueNotFoundError] if the queue does not exist
    # @raise [Fila::RPCError] for unexpected gRPC failures
    def submit(request)
      result_queue = Queue.new
      item = BatchItem.new(request: request, result_queue: result_queue)

      @mutex.synchronize do
        raise Fila::Error, 'batcher is closed' if @stopped

        @queue.push(item)
      end

      # Block until the batcher flushes our batch and posts the result.
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
          item = @queue.pop(true) # non_block = true
          if item == :shutdown
            @queue.push(:shutdown) # re-enqueue so the loop sees it
            break
          end
          batch << item
        rescue ThreadError
          break
        end
      end
    end

    # Flush a batch of items. Uses single Enqueue RPC for 1 message
    # (preserves exact error types like QueueNotFoundError), and
    # BatchEnqueue RPC for 2+ messages.
    def flush_batch(items)
      if items.size == 1
        flush_single(items.first)
      else
        flush_multi(items)
      end
    end

    def flush_single(item)
      resp = @stub.enqueue(item.request, metadata: @metadata)
      item.result_queue.push(resp.message_id)
    rescue GRPC::NotFound => e
      item.result_queue.push(QueueNotFoundError.new("enqueue: #{e.details}"))
    rescue GRPC::BadStatus => e
      item.result_queue.push(RPCError.new(e.code, e.details))
    rescue StandardError => e
      item.result_queue.push(Fila::Error.new(e.message))
    end

    def flush_multi(items)
      req = ::Fila::V1::BatchEnqueueRequest.new(
        messages: items.map(&:request)
      )
      resp = @stub.batch_enqueue(req, metadata: @metadata)
      results = resp.results

      items.each_with_index do |item, idx|
        result = results[idx]
        if result.nil?
          item.result_queue.push(Fila::Error.new('no result from server'))
        elsif result.result == :success
          item.result_queue.push(result.success.message_id)
        else
          item.result_queue.push(RPCError.new(GRPC::Core::StatusCodes::INTERNAL, result.error))
        end
      end
    rescue GRPC::BadStatus => e
      # Transport-level failure: all messages in this batch get the error.
      err = RPCError.new(e.code, e.details)
      items.each { |item| item.result_queue.push(err) }
    rescue StandardError => e
      err = Fila::Error.new(e.message)
      items.each { |item| item.result_queue.push(err) }
    end

    def current_time_ms
      (Process.clock_gettime(Process::CLOCK_MONOTONIC) * 1000).to_i
    end

    # Pop from @queue with a timeout in milliseconds.
    # Raises ThreadError if nothing is available within the timeout.
    def pop_with_timeout(timeout_ms)
      deadline = current_time_ms + timeout_ms
      loop do
        begin
          return @queue.pop(true)
        rescue ThreadError
          raise if current_time_ms >= deadline

          sleep(0.001) # 1ms polling interval
        end
      end
    end
  end
end
