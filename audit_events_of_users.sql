
-- Raw events table (retained for 30 days)
CREATE TABLE user_events
(
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;


-- Aggregated table (180-day retention)
CREATE TABLE user_events_daily_agg
(
    event_date Date,
    event_type String,
    unique_users AggregateFunction(uniq, UInt32),
    total_spent AggregateFunction(sum, UInt32),
    total_actions AggregateFunction(count)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;


-- Materialized View to populate aggregation table
CREATE MATERIALIZED VIEW user_events_mv
TO user_events_daily_agg
AS
SELECT
    toDate(event_time) AS event_date,
    event_type,
    uniqState(user_id) AS unique_users,
    sumState(points_spent) AS total_spent,
    countState() AS total_actions
FROM user_events
GROUP BY event_date, event_type;


-- Calculates the number of retained users within 7-day period
SELECT
    s.signup_date AS total_users_day_0,
    COUNT() AS total_signups,
    COUNT(DISTINCT r.user_id) AS returned_in_7_days,
    ROUND(COUNT(DISTINCT r.user_id) / COUNT() * 100, 2) AS retention_7d_percent
FROM (
    SELECT
        user_id,
        toDate(event_time) AS signup_date
    FROM user_events
    WHERE event_type = 'signup'
) AS s
LEFT JOIN (
    SELECT
        user_id,
        toDate(event_time) AS return_date
    FROM user_events
) AS r
ON s.user_id = r.user_id
   AND r.return_date > s.signup_date
   AND r.return_date <= s.signup_date + 7
GROUP BY s.signup_date
ORDER BY s.signup_date;

-- Query with daily aggregations for quick analytics (visualization-ready format)
SELECT
    event_date,
    event_type,
    uniqMerge(unique_users) AS unique_users,
    sumMerge(total_spent) AS total_spent,
    countMerge(total_actions) AS total_actions
FROM user_events_daily_agg
GROUP BY event_date, event_type
ORDER BY event_date, event_type;


-- INSERT operation
INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),

(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),

(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),

(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),

(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),

(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());




