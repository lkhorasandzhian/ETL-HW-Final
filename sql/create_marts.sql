CREATE SCHEMA IF NOT EXISTS etl;

-- ========================================================
-- Витрина 1. Активность пользователей
-- ========================================================
DROP MATERIALIZED VIEW IF EXISTS etl.user_activity_mart;

CREATE MATERIALIZED VIEW etl.user_activity_mart AS
WITH session_stats AS (
    SELECT
        us.user_id,
        COUNT(*) AS sessions_count,
        ROUND(SUM(us.session_duration_minutes), 2) AS total_session_minutes,
        ROUND(AVG(us.session_duration_minutes), 2) AS avg_session_minutes,
        MIN(us.start_time) AS first_activity_at,
        MAX(us.end_time) AS last_activity_at
    FROM etl.user_sessions us
    GROUP BY us.user_id
),
page_stats AS (
    SELECT
        us.user_id,
        COUNT(usp.id) AS total_page_visits,
        COUNT(DISTINCT usp.page_path) AS unique_pages_visited
    FROM etl.user_sessions us
    LEFT JOIN etl.user_session_pages usp
        ON us.session_id = usp.session_id
    GROUP BY us.user_id
),
action_stats AS (
    SELECT
        us.user_id,
        COUNT(usa.id) AS total_actions
    FROM etl.user_sessions us
    LEFT JOIN etl.user_session_actions usa
        ON us.session_id = usa.session_id
    GROUP BY us.user_id
),
top_action AS (
    SELECT user_id, action_name AS most_common_action
    FROM (
        SELECT
            us.user_id,
            usa.action_name,
            COUNT(*) AS action_count,
            ROW_NUMBER() OVER (
                PARTITION BY us.user_id
                ORDER BY COUNT(*) DESC, usa.action_name
            ) AS rn
        FROM etl.user_sessions us
        JOIN etl.user_session_actions usa
            ON us.session_id = usa.session_id
        GROUP BY us.user_id, usa.action_name
    ) ranked
    WHERE rn = 1
)
SELECT
    ss.user_id,
    ss.sessions_count,
    ss.total_session_minutes,
    ss.avg_session_minutes,
    COALESCE(ps.total_page_visits, 0) AS total_page_visits,
    COALESCE(ps.unique_pages_visited, 0) AS unique_pages_visited,
    COALESCE(ast.total_actions, 0) AS total_actions,
    COALESCE(ta.most_common_action, 'unknown') AS most_common_action,
    ss.first_activity_at,
    ss.last_activity_at
FROM session_stats ss
LEFT JOIN page_stats ps
    ON ss.user_id = ps.user_id
LEFT JOIN action_stats ast
    ON ss.user_id = ast.user_id
LEFT JOIN top_action ta
    ON ss.user_id = ta.user_id;


CREATE UNIQUE INDEX IF NOT EXISTS idx_user_activity_mart_user_id
    ON etl.user_activity_mart(user_id);


-- ========================================================
-- Витрина 2. Эффективность работы поддержки
-- ========================================================
DROP MATERIALIZED VIEW IF EXISTS etl.support_efficiency_mart;

CREATE MATERIALIZED VIEW etl.support_efficiency_mart AS
SELECT
    status,
    issue_type,
    COUNT(*) AS tickets_count,
    ROUND(AVG(resolution_time_minutes), 2) AS avg_resolution_time_minutes,
    ROUND(MIN(resolution_time_minutes), 2) AS min_resolution_time_minutes,
    ROUND(MAX(resolution_time_minutes), 2) AS max_resolution_time_minutes,
    COUNT(*) FILTER (WHERE status IN ('open', 'in_progress')) AS open_tickets_count,
    COUNT(*) FILTER (WHERE status IN ('resolved', 'closed')) AS resolved_or_closed_tickets_count,
    MIN(created_at) AS first_ticket_created_at,
    MAX(created_at) AS last_ticket_created_at
FROM etl.support_tickets
GROUP BY status, issue_type;


CREATE UNIQUE INDEX IF NOT EXISTS idx_support_efficiency_mart_status_issue_type
    ON etl.support_efficiency_mart(status, issue_type);