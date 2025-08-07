-- Telegram SMM Bot Database Schema
-- Version: 1.0.0
-- Description: Initial schema for automated SMM services

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Drop existing tables (for clean install)
DROP TABLE IF EXISTS execution_logs CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS posts CASCADE;
DROP TABLE IF EXISTS channel_settings CASCADE;
DROP TABLE IF EXISTS channels CASCADE;
DROP TABLE IF EXISTS twiboost_services CASCADE;

-- ============================================
-- 1. CHANNELS TABLE
-- ============================================
CREATE TABLE channels (
    id BIGINT PRIMARY KEY,  -- Telegram channel ID (negative for channels)
    username VARCHAR(255),  -- Channel username (without @)
    title VARCHAR(500) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    monitoring_interval INTEGER DEFAULT 30,  -- seconds
    last_check_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_channels_active ON channels(is_active) WHERE is_active = true;
CREATE INDEX idx_channels_last_check ON channels(last_check_at);

-- ============================================
-- 2. CHANNEL SETTINGS TABLE
-- ============================================
CREATE TABLE channel_settings (
    id SERIAL PRIMARY KEY,
    channel_id BIGINT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    service_type VARCHAR(50) NOT NULL, -- 'views', 'reactions', 'reposts'

    -- Base settings
    base_quantity INTEGER NOT NULL DEFAULT 100,
    randomization_percent INTEGER DEFAULT 0,  -- 0-100
    is_enabled BOOLEAN DEFAULT true,

    -- Portion settings for views
    portions_count INTEGER DEFAULT 5,
    fast_delivery_percent INTEGER DEFAULT 70,  -- 70% fast, 30% slow

    -- Reaction specific settings
    reaction_distribution JSONB,  -- {"👍": 45, "❤️": 30, "🔥": 25}

    -- Repost specific settings
    repost_delay_minutes INTEGER DEFAULT 5,

    -- Drip-feed settings
    drops_per_run INTEGER DEFAULT 5,  -- quantity per run
    run_interval INTEGER DEFAULT 30,  -- minutes between runs

    -- Twiboost service mappings
    twiboost_service_ids JSONB,  -- {"view_service": 1234, "reaction_thumbs": 5678}

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(channel_id, service_type)
);

CREATE INDEX idx_channel_settings_channel ON channel_settings(channel_id);
CREATE INDEX idx_channel_settings_enabled ON channel_settings(is_enabled) WHERE is_enabled = true;

-- ============================================
-- 3. POSTS TABLE
-- ============================================
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    channel_id BIGINT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    message_id INTEGER NOT NULL,
    content TEXT,
    media_type VARCHAR(50),  -- 'photo', 'video', 'text', etc.
    status VARCHAR(50) NOT NULL DEFAULT 'new',  -- 'new', 'processing', 'completed', 'failed'

    -- Stats
    views_count INTEGER DEFAULT 0,
    reactions_count INTEGER DEFAULT 0,
    reposts_count INTEGER DEFAULT 0,

    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,

    UNIQUE(channel_id, message_id)
);

CREATE INDEX idx_posts_status ON posts(status);
CREATE INDEX idx_posts_channel ON posts(channel_id);
CREATE INDEX idx_posts_detected ON posts(detected_at DESC);
CREATE INDEX idx_posts_new ON posts(status) WHERE status = 'new';

-- ============================================
-- 4. ORDERS TABLE
-- ============================================
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    twiboost_order_id BIGINT,  -- Order ID from Twiboost API

    service_type VARCHAR(50) NOT NULL,  -- 'views', 'reactions', 'reposts'
    service_id INTEGER NOT NULL,  -- Twiboost service ID

    -- Quantities
    quantity INTEGER NOT NULL,  -- Original requested
    actual_quantity INTEGER NOT NULL,  -- After randomization

    -- Portion info
    portion_number INTEGER DEFAULT 1,
    portion_size INTEGER,

    -- Drip-feed settings
    runs INTEGER,  -- Number of runs
    interval INTEGER,  -- Minutes between runs

    -- For reactions
    reaction_emoji VARCHAR(10),

    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    -- 'pending', 'in_progress', 'completed', 'awaiting', 'canceled', 'failed', 'partial'

    -- Timestamps
    scheduled_at TIMESTAMP WITH TIME ZONE,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,

    -- Response data
    response_data JSONB,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_orders_post ON orders(post_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_twiboost ON orders(twiboost_order_id) WHERE twiboost_order_id IS NOT NULL;
CREATE INDEX idx_orders_scheduled ON orders(scheduled_at) WHERE scheduled_at IS NOT NULL;
CREATE INDEX idx_orders_pending ON orders(status, scheduled_at) WHERE status = 'pending';

-- ============================================
-- 5. EXECUTION LOGS TABLE
-- ============================================
CREATE TABLE execution_logs (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
    action VARCHAR(100) NOT NULL,
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_logs_order ON execution_logs(order_id);
CREATE INDEX idx_logs_created ON execution_logs(created_at DESC);
CREATE INDEX idx_logs_action ON execution_logs(action);

-- ============================================
-- 6. TWIBOOST SERVICES TABLE (Cache)
-- ============================================
CREATE TABLE twiboost_services (
    service_id INTEGER PRIMARY KEY,
    name VARCHAR(500) NOT NULL,
    type VARCHAR(50) NOT NULL,
    category VARCHAR(500),
    rate DECIMAL(10, 2),  -- Price per 1000
    min_quantity INTEGER,
    max_quantity INTEGER,
    refill BOOLEAN DEFAULT false,
    cancel BOOLEAN DEFAULT false,
    is_active BOOLEAN DEFAULT true,
    synced_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Additional metadata
    metadata JSONB
);

CREATE INDEX idx_twiboost_type ON twiboost_services(type);
CREATE INDEX idx_twiboost_active ON twiboost_services(is_active) WHERE is_active = true;

-- ============================================
-- HELPER FUNCTIONS
-- ============================================

-- Function to update timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add update triggers
CREATE TRIGGER update_channels_updated_at BEFORE UPDATE ON channels
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_channel_settings_updated_at BEFORE UPDATE ON channel_settings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- STORED PROCEDURES
-- ============================================

-- Get pending orders for processing
CREATE OR REPLACE FUNCTION get_pending_orders(
    p_limit INTEGER DEFAULT 10
)
RETURNS TABLE (
    order_id INTEGER,
    post_id INTEGER,
    channel_id BIGINT,
    message_id INTEGER,
    service_type VARCHAR,
    service_id INTEGER,
    quantity INTEGER,
    scheduled_at TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        o.id as order_id,
        o.post_id,
        p.channel_id,
        p.message_id,
        o.service_type,
        o.service_id,
        o.actual_quantity as quantity,
        o.scheduled_at
    FROM orders o
    JOIN posts p ON o.post_id = p.id
    WHERE o.status = 'pending'
        AND (o.scheduled_at IS NULL OR o.scheduled_at <= NOW())
    ORDER BY o.created_at
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Update post statistics
CREATE OR REPLACE FUNCTION update_post_stats(
    p_post_id INTEGER,
    p_service_type VARCHAR,
    p_count INTEGER
)
RETURNS VOID AS $$
BEGIN
    UPDATE posts
    SET
        views_count = CASE WHEN p_service_type = 'views'
            THEN views_count + p_count ELSE views_count END,
        reactions_count = CASE WHEN p_service_type = 'reactions'
            THEN reactions_count + p_count ELSE reactions_count END,
        reposts_count = CASE WHEN p_service_type = 'reposts'
            THEN reposts_count + p_count ELSE reposts_count END
    WHERE id = p_post_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- SAMPLE DATA (for testing)
-- ============================================

-- Insert sample channel
-- INSERT INTO channels (id, username, title)
-- VALUES (-1001234567890, 'test_channel', 'Test Channel');

-- Insert sample settings
-- INSERT INTO channel_settings (channel_id, service_type, base_quantity, randomization_percent)
-- VALUES
-- (-1001234567890, 'views', 1000, 0),
-- (-1001234567890, 'reactions', 100, 40),
-- (-1001234567890, 'reposts', 50, 40);

-- ============================================
-- PERMISSIONS (adjust for your user)
-- ============================================
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO your_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO your_user;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO your_user;