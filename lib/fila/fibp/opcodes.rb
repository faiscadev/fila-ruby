# frozen_string_literal: true

module Fila
  module FIBP
    # Protocol opcodes matching the server's fila-fibp crate.
    module Opcodes
      # Control opcodes (0x00-0x0F)
      HANDSHAKE       = 0x01
      HANDSHAKE_OK    = 0x02
      PING            = 0x03
      PONG            = 0x04
      DISCONNECT      = 0x05

      # Hot-path opcodes (0x10-0x1F)
      ENQUEUE         = 0x10
      ENQUEUE_RESULT  = 0x11
      CONSUME         = 0x12
      CONSUME_OK      = 0x13
      DELIVERY        = 0x14
      CANCEL_CONSUME  = 0x15
      ACK             = 0x16
      ACK_RESULT      = 0x17
      NACK            = 0x18
      NACK_RESULT     = 0x19

      # Error opcode
      ERROR           = 0xFE

      # Admin opcodes (0xFD downward)
      CREATE_QUEUE        = 0xFD
      CREATE_QUEUE_RESULT = 0xFC
      DELETE_QUEUE        = 0xFB
      DELETE_QUEUE_RESULT = 0xFA
      GET_STATS           = 0xF9
      GET_STATS_RESULT    = 0xF8
      LIST_QUEUES         = 0xF7
      LIST_QUEUES_RESULT  = 0xF6
      SET_CONFIG          = 0xF5
      SET_CONFIG_RESULT   = 0xF4
      GET_CONFIG          = 0xF3
      GET_CONFIG_RESULT   = 0xF2
      LIST_CONFIG         = 0xF1
      LIST_CONFIG_RESULT  = 0xF0
      REDRIVE             = 0xEF
      REDRIVE_RESULT      = 0xEE
      CREATE_API_KEY        = 0xED
      CREATE_API_KEY_RESULT = 0xEC
      REVOKE_API_KEY        = 0xEB
      REVOKE_API_KEY_RESULT = 0xEA
      LIST_API_KEYS         = 0xE9
      LIST_API_KEYS_RESULT  = 0xE8
      SET_ACL               = 0xE7
      SET_ACL_RESULT        = 0xE6
      GET_ACL               = 0xE5
      GET_ACL_RESULT        = 0xE4
    end

    # Protocol error codes.
    module ErrorCodes
      OK                   = 0x00
      QUEUE_NOT_FOUND      = 0x01
      MESSAGE_NOT_FOUND    = 0x02
      QUEUE_ALREADY_EXISTS = 0x03
      LUA_COMPILATION      = 0x04
      STORAGE_ERROR        = 0x05
      NOT_A_DLQ            = 0x06
      PARENT_QUEUE_NOT_FOUND = 0x07
      INVALID_CONFIG_VALUE = 0x08
      CHANNEL_FULL         = 0x09
      UNAUTHORIZED         = 0x0A
      FORBIDDEN            = 0x0B
      NOT_LEADER           = 0x0C
      UNSUPPORTED_VERSION  = 0x0D
      INVALID_FRAME        = 0x0E
      API_KEY_NOT_FOUND    = 0x0F
      NODE_NOT_READY       = 0x10
      INTERNAL_ERROR       = 0xFF
    end

    # Continuation flag in frame flags byte.
    FLAG_CONTINUATION = 0x01
  end
end
