-- Lab 1: a continuous feed from the built-in sample data.
INSERT INTO `lq_orders`
SELECT `order_id`, `customer_id`, `product_id`, `price`, 'NEW', 'feed', CURRENT_TIMESTAMP
FROM `examples`.`marketplace`.`orders`;
