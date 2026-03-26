# frozen_string_literal: true

module Fila
  # Result of a single message within an enqueue_many call.
  #
  # Each message is independently validated and processed.
  # A failed message does not affect the others.
  #
  # @example
  #   results = client.enqueue_many(messages)
  #   results.each do |r|
  #     if r.success?
  #       puts "Enqueued: #{r.message_id}"
  #     else
  #       puts "Failed: #{r.error}"
  #     end
  #   end
  class EnqueueResult
    # @return [String, nil] broker-assigned message ID on success
    attr_reader :message_id

    # @return [String, nil] error description on failure
    attr_reader :error

    # @return [Integer, nil] FIBP error code on failure (nil on success)
    attr_reader :error_code

    # @param message_id [String, nil] message ID if successful
    # @param error [String, nil] error string if failed
    # @param error_code [Integer, nil] FIBP error code if failed
    def initialize(message_id: nil, error: nil, error_code: nil)
      @message_id = message_id
      @error      = error
      @error_code = error_code
    end

    # @return [Boolean] true if the message was successfully enqueued
    def success?
      !@message_id.nil?
    end
  end
end
