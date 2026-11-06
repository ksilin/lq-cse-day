-- Lab 2: one marker event. Replace {n} with 1, 2, 3 ... so each marker is unique.
INSERT INTO `lq_orders` VALUES ('MARKER-{n}', 0, 'marker', 0.0, 'MARKER', 'marker', CURRENT_TIMESTAMP);
