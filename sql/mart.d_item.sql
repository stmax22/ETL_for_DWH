INSERT INTO mart.d_item (
    item_id,
    item_name
)
SELECT DISTINCT
    item_id,
    item_name
FROM stage.user_order_log
WHERE item_id NOT IN (
	SELECT item_id
	FROM mart.d_item
);