# frozen_string_literal: true

module Fila
  # Base error for all Fila SDK errors.
  class Error < StandardError; end

  # Raised when the specified queue does not exist.
  class QueueNotFoundError < Error; end

  # Raised when the specified message does not exist.
  class MessageNotFoundError < Error; end

  # Raised for unexpected gRPC failures, preserving status code and message.
  class RPCError < Error
    # @return [Integer] gRPC status code
    attr_reader :code

    # @param code [Integer] gRPC status code
    # @param message [String] error message
    def initialize(code, message)
      @code = code
      super("rpc error (code = #{code}): #{message}")
    end
  end
end
