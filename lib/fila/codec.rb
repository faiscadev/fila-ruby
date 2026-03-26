# frozen_string_literal: true

module Fila
  # Encoding and decoding helpers for the FIBP binary wire format.
  #
  # All strings are UTF-8; all lengths and integers are big-endian.
  #
  # @api private
  module Codec # rubocop:disable Metrics/ModuleLength
    module_function

    # -----------------------------------------------------------------------
    # Enqueue request
    #
    # queue_len:u16BE | queue:utf8
    # msg_count:u16BE
    # messages... (each: header_count:u8 |
    #               headers: (key_len:u16BE+key, val_len:u16BE+val)* |
    #               payload_len:u32BE | payload)
    # -----------------------------------------------------------------------

    # @param queue [String]
    # @param messages [Array<Hash>] each has :payload and optional :headers
    # @return [String] binary payload
    def encode_enqueue(queue, messages)
      queue_b = queue.encode('UTF-8').b
      buf = [queue_b.bytesize].pack('n') + queue_b
      buf += [messages.size].pack('n')
      messages.each { |msg| buf += encode_message(msg) }
      buf
    end

    # @param payload [String] raw binary response payload
    # @return [Array<EnqueueResult>]
    def decode_enqueue_response(payload)
      pos = 0
      count, pos = read_u16(payload, pos)
      results = []
      count.times do
        ok, pos = read_u8(payload, pos)
        if ok == 1
          id_len, pos = read_u16(payload, pos)
          msg_id = payload.byteslice(pos, id_len).force_encoding('UTF-8')
          pos += id_len
          results << EnqueueResult.new(message_id: msg_id)
        else
          _err_code, pos = read_u16(payload, pos)
          err_len, pos = read_u16(payload, pos)
          err_msg = payload.byteslice(pos, err_len).force_encoding('UTF-8')
          pos += err_len
          results << EnqueueResult.new(error: err_msg)
        end
      end
      results
    end

    # -----------------------------------------------------------------------
    # Consume request
    #
    # queue_len:u16BE | queue:utf8 | initial_credits:u32BE
    # -----------------------------------------------------------------------

    # @param queue [String]
    # @param initial_credits [Integer]
    # @return [String] binary payload
    def encode_consume(queue, initial_credits: 256)
      queue_b = queue.encode('UTF-8').b
      [queue_b.bytesize].pack('n') + queue_b + [initial_credits].pack('N')
    end

    # Decode a server-push consume message frame payload.
    #
    # @param payload [String] raw binary frame payload
    # @return [ConsumeMessage, nil]
    def decode_consume_push(payload)
      pos = 0
      msg_id, pos         = read_str16(payload, pos)
      fairness_key, pos   = read_str16(payload, pos)
      attempt_count, pos  = read_u32(payload, pos)
      queue_id, pos       = read_str16(payload, pos)
      headers, pos        = read_headers(payload, pos)
      pay_len, pos        = read_u32(payload, pos)
      body                = payload.byteslice(pos, pay_len)

      ConsumeMessage.new(
        id: msg_id,
        headers: headers,
        payload: body,
        fairness_key: fairness_key,
        attempt_count: attempt_count,
        queue: queue_id
      )
    end

    # -----------------------------------------------------------------------
    # Ack request
    #
    # item_count:u16BE
    # items...: queue_len:u16BE+queue + msg_id_len:u16BE+msg_id
    # -----------------------------------------------------------------------

    # @param items [Array<Hash>] each has :queue and :msg_id
    # @return [String] binary payload
    def encode_ack(items)
      buf = [items.size].pack('n')
      items.each do |item|
        buf += encode_str16(item[:queue])
        buf += encode_str16(item[:msg_id])
      end
      buf
    end

    # Decode an ack response.
    # Response: item_count:u16 | items...: ok:u8 | if !ok: err_code:u16+err_len:u16+err_msg
    #
    # @param payload [String]
    # @return [Array<Hash>] each has :ok and optional :err_code, :err_msg
    def decode_ack_response(payload)
      pos = 0
      count, pos = read_u16(payload, pos)
      results = []
      count.times do
        ok, pos = read_u8(payload, pos)
        if ok == 1
          results << { ok: true }
        else
          err_code, pos = read_u16(payload, pos)
          err_len, pos  = read_u16(payload, pos)
          err_msg = payload.byteslice(pos, err_len).force_encoding('UTF-8')
          pos += err_len
          results << { ok: false, err_code: err_code, err_msg: err_msg }
        end
      end
      results
    end

    # -----------------------------------------------------------------------
    # Nack request
    #
    # Same as Ack but each item also has: err_len:u16BE+err_msg
    # -----------------------------------------------------------------------

    # @param items [Array<Hash>] each has :queue, :msg_id, :error
    # @return [String] binary payload
    def encode_nack(items)
      buf = [items.size].pack('n')
      items.each do |item|
        buf += encode_str16(item[:queue])
        buf += encode_str16(item[:msg_id])
        buf += encode_str16(item[:error].to_s)
      end
      buf
    end

    # Decode a nack response (same shape as ack response).
    alias decode_nack_response decode_ack_response

    private

    def encode_message(msg)
      headers = msg[:headers] || {}
      buf = [headers.size].pack('C')
      headers.each do |key, val|
        buf += encode_str16(key.to_s)
        buf += encode_str16(val.to_s)
      end
      payload_b = (msg[:payload] || '').b
      buf += [payload_b.bytesize].pack('N') + payload_b
      buf
    end

    def encode_str16(str)
      bytes = str.encode('UTF-8').b
      [bytes.bytesize].pack('n') + bytes
    end

    def read_str16(buf, pos)
      len, pos = read_u16(buf, pos)
      [buf.byteslice(pos, len).force_encoding('UTF-8'), pos + len]
    end

    def read_headers(buf, pos)
      count, pos = read_u8(buf, pos)
      headers = {}
      count.times do
        key, pos = read_str16(buf, pos)
        val, pos = read_str16(buf, pos)
        headers[key] = val
      end
      [headers, pos]
    end

    def read_u8(buf, pos)
      [buf.getbyte(pos), pos + 1]
    end

    def read_u16(buf, pos)
      [buf.byteslice(pos, 2).unpack1('n'), pos + 2]
    end

    def read_u32(buf, pos)
      [buf.byteslice(pos, 4).unpack1('N'), pos + 4]
    end
  end
end
