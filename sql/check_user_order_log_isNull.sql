SELECT COUNT(*)
FROM stage.user_order_log
WHERE customer_id IS NULL;