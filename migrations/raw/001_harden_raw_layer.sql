BEGIN;

-- 1️⃣ Add audit columns
ALTER TABLE raw.telegram_messages
ADD COLUMN IF NOT EXISTS ingestion_timestamp TIMESTAMPTZ DEFAULT now(),
ADD COLUMN IF NOT EXISTS source_file TEXT,
ADD COLUMN IF NOT EXISTS load_batch_id UUID DEFAULT gen_random_uuid();

-- 2️⃣ Enforce NOT NULL where appropriate
ALTER TABLE raw.telegram_messages
ALTER COLUMN message_id SET NOT NULL,
ALTER COLUMN channel SET NOT NULL,
ALTER COLUMN message_date SET NOT NULL;

-- 4️⃣ Indexes for analytics
CREATE INDEX IF NOT EXISTS idx_raw_channel ON raw.telegram_messages(channel);
CREATE INDEX IF NOT EXISTS idx_raw_message_date ON raw.telegram_messages(message_date);
CREATE INDEX IF NOT EXISTS idx_raw_views ON raw.telegram_messages(views);

COMMIT;
