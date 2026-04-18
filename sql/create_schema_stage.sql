DROP SCHEMA IF EXISTS stage CASCADE;

CREATE SCHEMA IF NOT EXISTS stage;

/* Создание таблицы customer_research. */
CREATE TABLE IF NOT EXISTS stage.customer_research (
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
	date_id TIMESTAMP,
	category_id INTEGER,
	geo_id INTEGER,
	sales_qty INTEGER,
	sales_amt NUMERIC(14, 2),
	status VARCHAR(10),
	CONSTRAINT customer_research_pk PRIMARY KEY (id)
);

/* Создание таблицы user_activity_log. */
CREATE TABLE IF NOT EXISTS stage.user_activity_log (
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
	uniq_id VARCHAR(32),
	date_time TIMESTAMP,
	action_id INTEGER,
	customer_id INTEGER,
	quantity INTEGER,
	status VARCHAR(10),
	CONSTRAINT user_activity_log_pk PRIMARY KEY (id)
);

/* Создание таблицы user_order_log. */
CREATE TABLE IF NOT EXISTS stage.user_order_log (
	id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
	uniq_id VARCHAR(32),
	date_time TIMESTAMP,
	city_id INTEGER,
	city_name VARCHAR(100),
	customer_id BIGINT,
	first_name VARCHAR(100),
	last_name VARCHAR(100),
	item_id INTEGER,
	item_name VARCHAR(100),
	quantity BIGINT,
	payment_amount NUMERIC(14, 2),
	status VARCHAR(10),
	CONSTRAINT user_order_log_pk PRIMARY KEY (id)
);
