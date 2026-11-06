-- Query the Tableflow (Iceberg) copy of your topic from DuckDB.
-- Replace {key}, {secret} (Global API key), {org}, {env}, {lkc}, {topic}.
-- The secret expires after 15 minutes (expires_in 900, HTTP 401): run the file again.
INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;
CREATE OR REPLACE SECRET iceberg_secret (TYPE ICEBERG, CLIENT_ID '{key}', CLIENT_SECRET '{secret}', ENDPOINT 'https://tableflow.eu-central-1.aws.confluent.cloud/iceberg/catalog/organizations/{org}/environments/{env}', OAUTH2_SCOPE 'catalog');
ATTACH 'warehouse' AS iceberg_catalog (TYPE iceberg, SECRET iceberg_secret, ENDPOINT 'https://tableflow.eu-central-1.aws.confluent.cloud/iceberg/catalog/organizations/{org}/environments/{env}');
SELECT count(*) AS rows_in_tableflow, max(produced_at) AS newest_produced_at FROM iceberg_catalog."{lkc}"."{topic}";
SELECT order_id, status, produced_at FROM iceberg_catalog."{lkc}"."{topic}" WHERE status = 'MARKER' ORDER BY produced_at DESC LIMIT 5;
