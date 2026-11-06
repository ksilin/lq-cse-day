-- Lab 3 (steps 7 and 9): one order for one customer, written with Flink. Replace {id}, {customer} and {price}.
-- Use it only when you have no Kafka key for the CLI producer. It takes about 10 s more, because the statement must start.
INSERT INTO `lq_orders` VALUES ('{id}', {customer}, 'marker', CAST({price} AS DOUBLE), 'NEW', 'marker', CURRENT_TIMESTAMP);
