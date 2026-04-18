SELECT COUNT(*)
FROM stage.user_activity_log
WHERE customer_id IS NULL;