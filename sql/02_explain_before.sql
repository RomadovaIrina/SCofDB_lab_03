\timing on
\echo '=== BEFORE OPTIMIZATION ==='

-- Рекомендуемые настройки для сравнимых замеров
SET max_parallel_workers_per_gather = 0;
SET work_mem = '32MB';
ANALYZE;

-- ============================================
-- TODO: Добавьте не менее 3 запросов
-- Для каждого обязательно: EXPLAIN (ANALYZE, BUFFERS)
-- ============================================

\echo '--- Q1: Фильтрация + сортировка (пример класса запроса) ---'
-- TODO: Подставьте свой запрос
-- Пример класса:
-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT ...
-- FROM orders
-- WHERE ...
-- ORDER BY created_at DESC
-- LIMIT ...;
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

\echo '--- Q2: Фильтрация по статусу + диапазону дат ---'
-- TODO: Подставьте свой запрос
-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT ...
-- FROM orders
-- WHERE status = 'paid'
--   AND created_at >= ...
--   AND created_at < ...;
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, user_id, total_amount, created_at
FROM orders
WHERE status = 'paid'
  AND created_at >= TIMESTAMP '2025-01-01'
  AND created_at < TIMESTAMP '2025-03-01'
ORDER BY created_at DESC
LIMIT 20;


\echo '--- Q3: JOIN + GROUP BY ---'
-- TODO: Подставьте свой запрос
-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT ...
-- FROM orders o
-- JOIN order_items oi ON oi.order_id = o.id
-- WHERE ...
-- GROUP BY ...
-- ORDER BY ...
-- LIMIT ...;
EXPLAIN (ANALYZE, BUFFERS)
SELECT o.id, COUNT(*) AS items_count, SUM(oi.price * oi.quantity) AS order_sum
FROM orders o
JOIN order_items oi ON oi.order_id = o.id
WHERE o.status = 'paid'
GROUP BY o.id
ORDER BY order_sum DESC
LIMIT 20;

-- (Опционально) Q4: полный агрегат по периоду, который сложно ускорить индексами
\echo '--- Q4 (optional) ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    SUM(total_amount) AS total_,
    COUNT(*) AS orders_count
FROM orders 
WHERE status = 'paid'
  AND created_at >= TIMESTAMP '2025-01-01'
  AND created_at < TIMESTAMP '2025-03-01';