INSERT INTO mart.f_sales (
	date_id,
	item_id,
	customer_id,
	city_id,
	quantity,
	payment_amount,
	order_status
)
SELECT 
	dc.date_id,
	uol.item_id,
	uol.customer_id,
	uol.city_id,
    CASE
    	WHEN COALESCE(uol.status, 'shipped') = 'refunded'
    	-- Если товар имеет статус "refunded", то делаем количество товара отрицательным.
    	THEN uol.quantity * -1
    	-- Если товар имеет статус "shipped", то количество товара остается положительным.
    	ELSE uol.quantity
    END AS quantity,
	CASE
		WHEN COALESCE(status, 'shipped') = 'refunded'
		-- Если товар имеет статус "refunded", то делаем сумму платежа отрицательной.
		THEN uol.payment_amount * -1
		-- Если товар имеет статус "shipped", то сумма платежа остается положительной.
		ELSE uol.payment_amount
	END AS payment_amount,
	uol.status AS order_status
FROM stage.user_order_log AS uol
LEFT JOIN mart.d_calendar AS dc on uol.date_time = dc.date_id
WHERE uol.date_time = '{{ ds }}';