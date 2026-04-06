\timing on
\echo '=== PARTITION ORDERS BY DATE ==='

-- ============================================
-- TODO: Реализуйте партиционирование orders по дате
-- ============================================

-- Вариант A (рекомендуется): RANGE по created_at (месяц/квартал)
-- Вариант B: альтернативная разумная стратегия

-- Шаг 1: Подготовка структуры
-- TODO:
-- - создайте partitioned table (или shadow-таблицу для безопасной миграции)
-- - определите partition key = created_at

-- Шаг 2: Создание партиций
-- TODO:
-- - создайте набор партиций по диапазонам дат
-- - добавьте DEFAULT partition (опционально)

-- Шаг 3: Перенос данных
-- TODO:
-- - перенесите данные из исходной таблицы
-- - проверьте количество строк до/после

-- Шаг 4: Индексы на партиционированной таблице
-- TODO:
-- - создайте нужные индексы (если требуется)

-- Шаг 5: Проверка
-- TODO:
-- - ANALYZE
-- - проверка partition pruning на запросах по диапазону дат


CREATE TABLE orders_partitioned_test (
  id UUID NOT NULL,
  user_id UUID NOT NULL REFERENCES users(id),
  status VARCHAR(50) NOT NULL REFERENCES order_statuses(status),
  total_amount NUMERIC(13, 2) NOT NULL CHECK (total_amount >= 0),
  created_at TIMESTAMP NOT NULL
) PARTITION BY RANGE (created_at);

CREATE TABLE orders_test_2024_q1 PARTITION OF orders_partitioned_test
FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

CREATE TABLE orders_test_2024_q2 PARTITION OF orders_partitioned_test
FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');

CREATE TABLE orders_test_2024_q3 PARTITION OF orders_partitioned_test
FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');

CREATE TABLE orders_test_2024_q4 PARTITION OF orders_partitioned_test
FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');

CREATE TABLE orders_test_2025_q1 PARTITION OF orders_partitioned_test
FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');

CREATE TABLE orders_test_2025_q2 PARTITION OF orders_partitioned_test
FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');

CREATE TABLE orders_test_2025_q3 PARTITION OF orders_partitioned_test
FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');

CREATE TABLE orders_test_2025_q4 PARTITION OF orders_partitioned_test
FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');

CREATE TABLE orders_test_default PARTITION OF orders_partitioned_test DEFAULT;

\echo 'some copies'
INSERT INTO orders_partitioned_test (id, user_id, status, total_amount, created_at)
SELECT id, user_id, status, total_amount, created_at
FROM orders;

CREATE INDEX idx_orders_demo_created_at ON 
orders_partitioned_test USING BTREE (created_at);

CREATE INDEX idx_orders_demo_status_created ON 
orders_partitioned_test USING BTREE (status, created_at DESC);

ANALYZE orders_partitioned_test;

\echo 'before partitioning:'
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*), AVG(total_amount)
FROM orders
WHERE created_at >= '2025-01-01'
  AND created_at < '2025-04-01';

\echo 'after partitioning:'
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*), AVG(total_amount)
FROM orders_partitioned_test
WHERE created_at >= '2025-01-01'
  AND created_at < '2025-04-01';

\echo 'done partitioning test'