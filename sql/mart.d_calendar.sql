WITH all_dates AS (
	SELECT
	    cr.date_id
	FROM stage.customer_research AS cr
	
	UNION
	
	SELECT
	    uol.date_time AS date_id
	FROM stage.user_order_log AS uol
	
	UNION
	
	SELECT
	    ual.date_time AS date_id
	FROM stage.user_activity_log AS ual
	)
	
INSERT INTO mart.d_calendar (
    date_id,
    day_num,
    month_num,
    month_name,
    year_num
)
SELECT
    date_id,
    EXTRACT(DAY FROM date_id) AS day_num,
    EXTRACT(MONTH FROM date_id) AS month_num,
    CASE EXTRACT(MONTH FROM date_id)
        WHEN 1 THEN 'Январь'
        WHEN 2 THEN 'Февраль'
        WHEN 3 THEN 'Март'
        WHEN 4 THEN 'Апрель'
        WHEN 5 THEN 'Май'
        WHEN 6 THEN 'Июнь'
        WHEN 7 THEN 'Июль'
        WHEN 8 THEN 'Август'
        WHEN 9 THEN 'Сентябрь'
        WHEN 10 THEN 'Октябрь'
        WHEN 11 THEN 'Ноябрь'
        WHEN 12 THEN 'Декабрь'
    END AS month_name,
    EXTRACT(YEAR FROM date_id) AS year_num
FROM all_dates
WHERE date_id NOT IN (SELECT date_id from mart.d_calendar);