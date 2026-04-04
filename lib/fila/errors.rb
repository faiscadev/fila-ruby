# frozen_string_literal: true

module Fila
  # Base error for all Fila SDK errors.
  class Error < StandardError; end

  # Raised when the specified queue does not exist.
  class QueueNotFoundError < Error; end

  # Raised when the specified message does not exist.
  class MessageNotFoundError < Error; end

  # Raised when the queue already exists.
  class QueueAlreadyExistsError < Error; end

  # Raised when authentication fails (missing or invalid API key).
  class AuthenticationError < Error; end

  # Raised when the client lacks permission for the operation.
  class ForbiddenError < Error; end

  # Raised when the server is not the leader for the target queue.
  class NotLeaderError < Error
    # @return [String, nil] address of the current leader
    attr_reader :leader_addr

    # @param message [String] error message
    # @param leader_addr [String, nil] leader address hint
    def initialize(message, leader_addr: nil)
      @leader_addr = leader_addr
      super(message)
    end
  end

  # Raised when the API key is not found.
  class ApiKeyNotFoundError < Error; end

  # Raised for transport or protocol failures, preserving the error code.
  class RPCError < Error
    # @return [Integer] FIBP error code
    attr_reader :code

    # @param code [Integer] error code
    # @param message [String] error message
    def initialize(code, message)
      @code = code
      super("rpc error (code = 0x#{code.to_s(16).rjust(2, '0')}): #{message}")
    end
  end
end
