INSERT INTO mart.d_customer (
    customer_id,
    first_name,
    last_name,
    city_id
)
SELECT DISTINCT
    customer_id,
    first_name,
    last_name,
    MAX(city_id) OVER (PARTITION BY customer_id) AS city_id
FROM stage.user_order_log
WHERE customer_id NOT IN (
	SELECT customer_id
	FROM mart.d_customer
);