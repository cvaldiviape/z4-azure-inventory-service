UPDATE outbox_events
SET topic = 'inventory-reservation-results-topic'
WHERE status = 'PENDING'
  AND topic = 'inventory-events-topic';
