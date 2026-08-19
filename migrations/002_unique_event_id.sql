DELETE FROM events older
USING events newer
WHERE older.event_id = newer.event_id
  AND older.id > newer.id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_events_event_id ON events (event_id);
