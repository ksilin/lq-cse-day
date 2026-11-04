-- Lab 1: the append table for orders.
-- No watermark and no computed column: RTCE rejects them (MT_SCHEMA_ERROR).
CREATE TABLE IF NOT EXISTS `lq_orders` (
  `order_id` STRING,
  `customer_id` INT,
  `product_id` STRING,
  `price` DOUBLE,
  `status` STRING,
  `source` STRING,
  `produced_at` TIMESTAMP_LTZ(3)
) WITH (
  'changelog.mode' = 'append',
  'kafka.cleanup-policy' = 'delete',
  'value.format' = 'avro-registry'
);
