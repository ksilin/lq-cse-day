-- Lab 3 path A: the upsert table, one current row per customer.
-- RTCE needs a raw key, and Flink needs the raw key column to be named `key`.
CREATE TABLE IF NOT EXISTS `lq_orders_latest` (
  `key` STRING NOT NULL,
  `customer_id` INT,
  `order_id` STRING,
  `price` DOUBLE,
  `status` STRING,
  `produced_at` TIMESTAMP_LTZ(3),
  PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY (`key`) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'compact',
  'key.format' = 'raw',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all',
  'value.format' = 'avro-registry'
);
