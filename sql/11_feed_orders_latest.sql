-- Lab 3 path A: keep the newest order per customer.
-- The HAVING clause is for the delete exercise: a CANCELLED newest order removes the key.
-- STATE_TTL 4h: a delete (a CANCELLED order) works only while Flink still has the state of the key.
INSERT INTO `lq_orders_latest`
SELECT /*+ STATE_TTL('o' = '4h') */
  CAST(`customer_id` AS STRING) AS `key`,
  LAST_VALUE(`customer_id`) AS `customer_id`,
  LAST_VALUE(`order_id`) AS `order_id`,
  LAST_VALUE(`price`) AS `price`,
  LAST_VALUE(`status`) AS `status`,
  LAST_VALUE(`produced_at`) AS `produced_at`
FROM `lq_orders` AS `o`
GROUP BY CAST(`customer_id` AS STRING)
HAVING LAST_VALUE(`status`) <> 'CANCELLED';
