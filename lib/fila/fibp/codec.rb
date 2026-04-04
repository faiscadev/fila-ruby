# frozen_string_literal: true

module Fila
  module FIBP
    # Low-level encoding/decoding primitives for the Fila binary protocol.
    #
    # All multi-byte integers are big-endian (network byte order).
    # Strings are length-prefixed with u16; bytes with u32; maps with u16 count.
    module Codec
      module_function

      # --- Encoding primitives ---

      def encode_u8(val)
        [val].pack('C')
      end

      def encode_u16(val)
        [val].pack('n')
      end

      def encode_u32(val)
        [val].pack('N')
      end

      def encode_u64(val)
        [val >> 32, val & 0xFFFFFFFF].pack('NN')
      end

      def encode_i64(val)
        [val >> 32, val & 0xFFFFFFFF].pack('NN')
      end

      def encode_f64(val)
        [val].pack('G')
      end

      def encode_bool(val)
        [val ? 1 : 0].pack('C')
      end

      def encode_string(str)
        bytes = str.to_s.encode('UTF-8').b
        [bytes.bytesize].pack('n') + bytes
      end

      def encode_bytes(data)
        raw = data.to_s.b
        [raw.bytesize].pack('N') + raw
      end

      def encode_map(hash)
        hash ||= {}
        buf = [hash.size].pack('n')
        hash.each do |k, v|
          buf += encode_string(k) + encode_string(v)
        end
        buf
      end

      def encode_string_array(arr)
        arr ||= []
        buf = [arr.size].pack('n')
        arr.each { |s| buf += encode_string(s) }
        buf
      end

      def encode_optional_string(val)
        if val.nil?
          encode_u8(0)
        else
          encode_u8(1) + encode_string(val)
        end
      end

      # Build a frame: [u32 frame_length][u8 opcode][u8 flags][u32 request_id][payload]
      def encode_frame(opcode, request_id, payload, flags: 0)
        body = [opcode, flags, request_id].pack('CCN') + payload
        [body.bytesize].pack('N') + body
      end

      # --- Decoding primitives ---

      # Reader wraps a binary string with a cursor for sequential reads.
      class Reader
        attr_reader :pos

        def initialize(data)
          @data = data.b
          @pos = 0
        end

        def remaining
          @data.bytesize - @pos
        end

        def read_raw(n)
          raise ProtocolError, "unexpected end of frame (need #{n}, have #{remaining})" if remaining < n

          slice = @data.byteslice(@pos, n)
          @pos += n
          slice
        end

        def read_u8
          read_raw(1).unpack1('C')
        end

        def read_u16
          read_raw(2).unpack1('n')
        end

        def read_u32
          read_raw(4).unpack1('N')
        end

        def read_u64
          hi, lo = read_raw(8).unpack('NN')
          (hi << 32) | lo
        end

        def read_i64
          hi, lo = read_raw(8).unpack('NN')
          val = (hi << 32) | lo
          val >= (1 << 63) ? val - (1 << 64) : val
        end

        def read_f64
          read_raw(8).unpack1('G')
        end

        def read_bool
          read_u8 != 0
        end

        def read_string
          len = read_u16
          read_raw(len).force_encoding('UTF-8')
        end

        def read_bytes
          len = read_u32
          read_raw(len)
        end

        def read_map
          count = read_u16
          hash = {}
          count.times do
            k = read_string
            v = read_string
            hash[k] = v
          end
          hash
        end

        def read_string_array
          count = read_u16
          Array.new(count) { read_string }
        end

        def read_optional_string
          present = read_u8
          present == 1 ? read_string : nil
        end
      end
    end

    # Raised for protocol-level decode errors.
    class ProtocolError < Fila::Error; end
  end
end
