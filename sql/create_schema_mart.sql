DROP SCHEMA IF EXISTS mart CASCADE;

CREATE SCHEMA IF NOT EXISTS mart;

/* Создание таблицы измерения d_calendar. */
CREATE TABLE IF NOT EXISTS mart.d_calendar (
	date_id TIMESTAMP NOT NULL,
	day_num SMALLINT NOT NULL,
	month_num SMALLINT NOT NULL,
	month_name VARCHAR(10) NOT NULL,
	year_num SMALLINT NOT NULL,
	CONSTRAINT d_calendar_pk PRIMARY KEY (date_id)
);

/* Создание таблицы измерения d_city. */
CREATE TABLE IF NOT EXISTS mart.d_city (
	city_id INTEGER NOT NULL,
	city_name VARCHAR(50) NOT NULL,
	CONSTRAINT d_city_pk PRIMARY KEY (city_id)
);

/* Создание таблицы измерения d_customer. */
CREATE TABLE IF NOT EXISTS mart.d_customer (
	customer_id INTEGER NOT NULL,
	first_name VARCHAR(20) NOT NULL,
	last_name VARCHAR(20) NULL,
	city_id INTEGER NOT NULL,
	CONSTRAINT d_customer_pk PRIMARY KEY (customer_id)
);

/* Создание таблицы измерения d_item. */
CREATE TABLE IF NOT EXISTS mart.d_item (
	item_id INTEGER NOT NULL,
	item_name VARCHAR(100) NOT NULL,
	CONSTRAINT d_item_pk PRIMARY KEY (item_id)
);

/* Создание таблицы фактов f_sales. */
CREATE TABLE IF NOT EXISTS mart.f_sales (
	date_id TIMESTAMP NOT NULL,
	item_id INTEGER NOT NULL,
	customer_id INTEGER NOT NULL,
	city_id INTEGER NOT NULL,
	quantity INTEGER NOT NULL,
	payment_amount NUMERIC(14, 2) NOT NULL,
	order_status VARCHAR(10) NOT NULL
);

/* Создание витрины f_customer_retention. */
CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    new_customers_count SMALLINT NOT NULL,
    returning_customers_count SMALLINT NOT NULL,
    refunded_customer_count SMALLINT NOT NULL,
    period_name VARCHAR(10) NOT NULL,
    period_id SMALLINT NOT NULL,
    item_id INTEGER NOT NULL,
    new_customers_revenue SMALLINT NULL,
    returning_customers_revenue SMALLINT NULL,
    customers_refunded SMALLINT NOT NULL
);
