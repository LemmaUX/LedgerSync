-- LedgerSync Database Initialization Script
-- This script initializes the PostgreSQL database for CDC replication

-- Enable logical replication (already set via docker-compose command)
-- wal_level = logical
-- max_replication_slots = 4
-- max_wal_senders = 4

-- Create transactions table for internal ledger
CREATE TABLE IF NOT EXISTS transactions (
    id BIGSERIAL PRIMARY KEY,
    uuid UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    amount_cents BIGINT NOT NULL,
    currency_code VARCHAR(3) NOT NULL DEFAULT 'USD',
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    type VARCHAR(50) NOT NULL DEFAULT 'PAYMENT',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create index for CDC performance
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_transactions_uuid ON transactions(uuid);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to auto-update updated_at
DROP TRIGGER IF EXISTS update_transactions_updated_at ON transactions;
CREATE TRIGGER update_transactions_updated_at
    BEFORE UPDATE ON transactions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions for Debezium CDC
GRANT SELECT ON pg_catalog.pg_class TO ledgersync;
GRANT SELECT ON pg_catalog.pg_namespace TO ledgersync;
GRANT SELECT ON pg_catalog.pg_attribute TO ledgersync;
GRANT SELECT ON pg_catalog.pg_type TO ledgersync;

-- Create publication for Debezium to capture changes
DROP PUBLICATION IF EXISTS ledgersync_publication;
CREATE PUBLICATION ledgersync_publication FOR TABLE transactions;

-- Insert sample data for testing
INSERT INTO transactions (uuid, amount_cents, currency_code, status, type, created_at) VALUES
    ('a1b2c3d4-e5f6-7890-abcd-ef1234567890', 10000, 'USD', 'COMPLETED', 'PAYMENT', NOW() - INTERVAL '1 hour'),
    ('b2c3d4e5-f6a7-8901-bcde-f12345678901', 5000, 'EUR', 'COMPLETED', 'PAYMENT', NOW() - INTERVAL '30 minutes'),
    ('c3d4e5f6-a7b8-9012-cdef-123456789012', 7500, 'USD', 'PENDING', 'REFUND', NOW() - INTERVAL '15 minutes');
