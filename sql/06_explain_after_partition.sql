\timing on
\echo '=== AFTER PARTITIONING ==='

SET max_parallel_workers_per_gather = 0;
SET work_mem = '32MB';

-- TODO:
-- Выполните ANALYZE для партиционированной таблицы/таблиц
-- Пример:
-- ANALYZE orders;

-- ============================================
-- TODO:
-- Скопируйте сюда те же запросы, что в:
--   02_explain_before.sql
--   04_explain_after_indexes.sql
-- и выполните EXPLAIN (ANALYZE, BUFFERS) после партиционирования.
-- ============================================

\echo '--- Q1 ---'
-- TODO: EXPLAIN (ANALYZE, BUFFERS) ...
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, user_id, total_amount, created_at
FROM orders
WHERE status = 'paid'
  AND total_amount > 3000
  AND id IN (
      SELECT order_id
      FROM order_items
      WHERE price > 500
  )
ORDER BY created_at DESC
LIMIT 20;



\echo '--- Q2 ---'
-- TODO: EXPLAIN (ANALYZE, BUFFERS) ...
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, user_id, total_amount, created_at
FROM orders
WHERE status = 'paid'
  AND created_at >= TIMESTAMP '2025-01-01'
  AND created_at < TIMESTAMP '2025-07-01'
ORDER BY created_at DESC
LIMIT 20;


\echo '--- Q3 ---'
-- TODO: EXPLAIN (ANALYZE, BUFFERS) ...
EXPLAIN (ANALYZE, BUFFERS)
SELECT o.id, COUNT(*) AS items_count, SUM(oi.price * oi.quantity) AS order_sum
FROM orders o
JOIN order_items oi ON oi.order_id = o.id
WHERE o.status = 'paid'
GROUP BY o.id
ORDER BY order_sum DESC
LIMIT 20;


\echo '--- Q4 (optional) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    SUM(total_amount) AS total_,
    COUNT(*) AS orders_count,
FROM orders
WHERE status = 'paid'
  AND created_at >= TIMESTAMP '2025-01-01'
  AND created_at < TIMESTAMP '2025-03-01';
-- (Опционально) Q4
-- TODO
