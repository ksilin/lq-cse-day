-- Lab 4 (O2): a CANCELLED order for one customer, written with Flink. Replace {n} and {customer}.
-- Use it only when you have no Kafka key for the CLI producer (Lab 4, O2).
INSERT INTO `lq_orders` VALUES ('CANCEL-{n}', {customer}, 'cancel', 0.0, 'CANCELLED', 'marker', CURRENT_TIMESTAMP);
