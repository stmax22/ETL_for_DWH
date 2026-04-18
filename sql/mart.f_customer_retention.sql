WITH
sales_data AS (
    SELECT DISTINCT 
        customer_id,
        item_id,
        date_id,
        quantity,
        payment_amount,
        COUNT(*) OVER my_window AS order_count,
        SUM(payment_amount) OVER my_window AS total_revenue,
        SUM(CASE
	        	WHEN order_status = 'refunded'
	        	THEN quantity
	        	ELSE 0 
	        END) OVER my_window AS refunded_quantity
    FROM mart.f_sales
    WINDOW my_window AS (PARTITION BY customer_id, item_id, date_id)
),
weekly_data AS (
	SELECT 
	    date_id::DATE - (EXTRACT(DOW FROM date_id) + 6)::SMALLINT % 7 AS week_start,  -- Начало недели (понедельник).
	    EXTRACT(WEEK FROM date_id) AS week_number,  -- Номер недели.
	    EXTRACT(YEAR FROM date_id) AS year_number  -- Номер года.
	FROM mart.f_sales
	GROUP BY week_start, week_number, year_number 
),
customers_data AS (
    SELECT 
        sd.customer_id,
        wd.week_number,
        wd.year_number,
        sd.item_id,
        COUNT(DISTINCT sd.customer_id) FILTER (WHERE sd.order_count = 1) AS new_customers_count,
        COUNT(DISTINCT sd.customer_id) FILTER (WHERE sd.order_count > 1) AS returning_customers_count,
        SUM(sd.total_revenue) FILTER (WHERE sd.order_count = 1) AS new_customers_revenue,
        SUM(sd.total_revenue) FILTER (WHERE sd.order_count > 1) AS returning_customers_revenue,
        SUM(sd.refunded_quantity) AS customers_refunded
    FROM sales_data AS sd
    JOIN weekly_data AS wd ON sd.date_id = wd.week_start
    GROUP BY sd.customer_id, sd.item_id, wd.week_number, wd.year_number
)

INSERT INTO mart.f_customer_retention (
    new_customers_count,
    returning_customers_count,
    refunded_customer_count,
    period_name,
    period_id,
    item_id,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded
)
SELECT 
    new_customers_count,
    returning_customers_count,
    customers_refunded,
    'weekly' AS period_name,
    week_number AS period_id,
    item_id,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded
FROM customers_data;
