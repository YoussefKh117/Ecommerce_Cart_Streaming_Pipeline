-- ============================================================
-- CartStreamAnalysis - Apache Flink SQL Pipeline
-- ============================================================
-- ============================================================
-- 1. SOURCE TABLE
-- Kafka topic: ecommerce_events
-- ============================================================
CREATE TABLE ecommerce_events (
    event_time_str STRING,
    event_time AS TO_TIMESTAMP(
        event_time_str,
        'yyyy-MM-dd HH:mm:ss z'
    ),
    event_type STRING,
    product_id BIGINT,
    category_code STRING,
    brand STRING,
    price DECIMAL(10, 2),
    user_id BIGINT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'ecommerce_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-cartstream',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
-- ============================================================
-- 2. SINK TABLE
-- Print sink for execution verification
-- ============================================================
CREATE TABLE brand_window_sales (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    brand STRING,
    total_orders BIGINT,
    gross_revenue DECIMAL(20, 2),
    avg_order_value DECIMAL(20, 2),
    unique_buyers BIGINT
) WITH ('connector' = 'print');
-- ============================================================
-- 3. 5-MINUTE TUMBLING WINDOW
-- Purchase aggregation by brand
-- ============================================================
INSERT INTO brand_window_sales
SELECT window_start,
    window_end,
    brand,
    COUNT(*) AS total_orders,
    CAST(
        ROUND(SUM(price), 2) AS DECIMAL(20, 2)
    ) AS gross_revenue,
    CAST(
        ROUND(AVG(price), 2) AS DECIMAL(20, 2)
    ) AS avg_order_value,
    COUNT(DISTINCT user_id) AS unique_buyers
FROM TABLE(
        TUMBLE(
            TABLE ecommerce_events,
            DESCRIPTOR(event_time),
            INTERVAL '5' MINUTES
        )
    )
WHERE event_type = 'purchase'
GROUP BY window_start,
    window_end,
    brand;