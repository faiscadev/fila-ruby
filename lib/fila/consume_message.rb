# frozen_string_literal: true

module Fila
  # A message received from the broker via a consume stream.
  ConsumeMessage = Struct.new(
    :id,            # Broker-assigned message ID (UUIDv7).
    :headers,       # Message headers (Hash<String, String>).
    :payload,       # Message payload bytes (String).
    :fairness_key,  # Fairness key for scheduling.
    :attempt_count, # Number of previous delivery attempts.
    :queue,         # Queue the message belongs to.
    keyword_init: true
  )
end
