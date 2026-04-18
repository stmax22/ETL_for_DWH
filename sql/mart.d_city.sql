INSERT INTO mart.d_city (
	city_id,
	city_name
	)
SELECT DISTINCT
    city_id,
    city_name
FROM stage.user_order_log
WHERE city_id NOT IN (
	SELECT city_id
	FROM mart.d_city
);