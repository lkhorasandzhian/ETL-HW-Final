CREATE SCHEMA IF NOT EXISTS etl;

-- ========================================================
-- 1.UserSessions, сессии пользователей
-- ========================================================
CREATE TABLE IF NOT EXISTS etl.user_sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    device VARCHAR(50),
    session_duration_minutes NUMERIC(10,2) GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (end_time - start_time)) / 60.0
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id
    ON etl.user_sessions(user_id);

CREATE INDEX IF NOT EXISTS idx_user_sessions_start_time
    ON etl.user_sessions(start_time);


CREATE TABLE IF NOT EXISTS etl.user_session_pages (
    id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(50) NOT NULL REFERENCES etl.user_sessions(session_id) ON DELETE CASCADE,
    page_path TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_session_pages_session_id
    ON etl.user_session_pages(session_id);

CREATE INDEX IF NOT EXISTS idx_user_session_pages_page_path
    ON etl.user_session_pages(page_path);


CREATE TABLE IF NOT EXISTS etl.user_session_actions (
    id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(50) NOT NULL REFERENCES etl.user_sessions(session_id) ON DELETE CASCADE,
    action_name VARCHAR(100) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_session_actions_session_id
    ON etl.user_session_actions(session_id);

CREATE INDEX IF NOT EXISTS idx_user_session_actions_action_name
    ON etl.user_session_actions(action_name);


-- ========================================================
-- 2. EventLogs, логи событий
-- ========================================================
CREATE TABLE IF NOT EXISTS etl.event_logs (
    event_id VARCHAR(50) PRIMARY KEY,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    details TEXT
);

CREATE INDEX IF NOT EXISTS idx_event_logs_timestamp
    ON etl.event_logs(event_timestamp);

CREATE INDEX IF NOT EXISTS idx_event_logs_event_type
    ON etl.event_logs(event_type);


-- ========================================================
-- 3. SupportTickets, обращения в поддержку
-- ========================================================
CREATE TABLE IF NOT EXISTS etl.support_tickets (
    ticket_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    issue_type VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    resolution_time_minutes NUMERIC(10,2) GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (updated_at - created_at)) / 60.0
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_support_tickets_user_id
    ON etl.support_tickets(user_id);

CREATE INDEX IF NOT EXISTS idx_support_tickets_status
    ON etl.support_tickets(status);

CREATE INDEX IF NOT EXISTS idx_support_tickets_issue_type
    ON etl.support_tickets(issue_type);


CREATE TABLE IF NOT EXISTS etl.support_ticket_messages (
    id BIGSERIAL PRIMARY KEY,
    ticket_id VARCHAR(50) NOT NULL REFERENCES etl.support_tickets(ticket_id) ON DELETE CASCADE,
    sender VARCHAR(50) NOT NULL,
    message_text TEXT NOT NULL,
    message_timestamp TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_support_ticket_messages_ticket_id
    ON etl.support_ticket_messages(ticket_id);


-- ========================================================
-- 4. UserRecommendations, рекомендации пользователям
-- ========================================================
CREATE TABLE IF NOT EXISTS etl.user_recommendations (
    user_id VARCHAR(50) PRIMARY KEY,
    last_updated TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_recommendations_last_updated
    ON etl.user_recommendations(last_updated);


CREATE TABLE IF NOT EXISTS etl.user_recommendation_products (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL REFERENCES etl.user_recommendations(user_id) ON DELETE CASCADE,
    product_id VARCHAR(50) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_recommendation_products_user_id
    ON etl.user_recommendation_products(user_id);

CREATE INDEX IF NOT EXISTS idx_user_recommendation_products_product_id
    ON etl.user_recommendation_products(product_id);


-- ========================================================
-- 5. ModerationQueue, очередь модерации отзывов
-- ========================================================
CREATE TABLE IF NOT EXISTS etl.moderation_queue (
    review_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    review_text TEXT,
    rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    moderation_status VARCHAR(50) NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_moderation_queue_user_id
    ON etl.moderation_queue(user_id);

CREATE INDEX IF NOT EXISTS idx_moderation_queue_product_id
    ON etl.moderation_queue(product_id);

CREATE INDEX IF NOT EXISTS idx_moderation_queue_status
    ON etl.moderation_queue(moderation_status);


CREATE TABLE IF NOT EXISTS etl.moderation_queue_flags (
    id BIGSERIAL PRIMARY KEY,
    review_id VARCHAR(50) NOT NULL REFERENCES etl.moderation_queue(review_id) ON DELETE CASCADE,
    flag_name VARCHAR(100) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_moderation_queue_flags_review_id
    ON etl.moderation_queue_flags(review_id);

CREATE INDEX IF NOT EXISTS idx_moderation_queue_flags_flag_name
    ON etl.moderation_queue_flags(flag_name);