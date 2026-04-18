SELECT
    CASE WHEN COUNT(DISTINCT(customer_id)) > 3
        THEN TRUE
        ELSE FALSE
    END
FROM stage.user_order_log;