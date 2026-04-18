DROP SCHEMA IF EXISTS checks CASCADE;

CREATE SCHEMA IF NOT EXISTS checks;

/* Создание таблицы контроля. */
CREATE TABLE IF NOT EXISTS checks.dq_checks_results (
	table_name VARCHAR(50) NOT NULL,
	check_name VARCHAR(50) NOT NULL,
	check_date TIMESTAMP NOT NULL,
	check_status NUMERIC(1) NOT NULL
);