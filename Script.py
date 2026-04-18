import json
import logging
import pandas as pd
import requests
import time

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Задаем формат лог-сообщений.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Данные, прописанные в Airflow:
postgres_conn_id = 'pg_connection'  # Для подключения к БД.
file_search_local = 'fs_local'  # Для поиска файла по имени.
headers = Variable.get('headers', deserialize_json=True)  # Заголовки.
s3 = Variable.get('s3')  # Ссылка на S3.
conn_id = 'http_conn_id'  # Для подключение к API.
http_conn_id = BaseHook.get_connection(conn_id)
api_endpoint = http_conn_id.host


def generate_report(ti):
    """Метод создает запрос на создание файлов."""

    try:
        logging.info('Отправляем запрос на получение строкового идентификатора "task_id".')

        method_url = '/generate_report'
        response = requests.post(api_endpoint + method_url, headers=headers)
        response.raise_for_status()
        response_dict = json.loads(response.content)
        task_id = response_dict['task_id']

        ti.xcom_push(key='task_id', value=task_id)
        logging.info(f'По результату запроса task_id={task_id}.')

    except Exception:
        logging.exception('Ошибка при отправке запроса на получение строкового идентификатора "task_id".')
        raise


def get_report(ti):
    """Метод проверяет готовность файлов после запроса на их создание."""

    try:
        logging.info('Отправляем запрос на получение строкового идентификатора "report_id".')

        task_id = ti.xcom_pull(key='task_id')
        payload = {'task_id': task_id}

        report_id = None  # Cтроковый идентификатор.

        for i in range(4):
            time.sleep(70)  # Отчёт выгружается 60 секунд минимум.

            method_url = '/get_report'
            response = requests.get(api_endpoint + method_url, params=payload, headers=headers)
            response.raise_for_status()
            response_dict = json.loads(response.content)
            status = response_dict['status']

            logging.info(f'Статус {status} c {i + 1} итерации.')

            if status == 'SUCCESS':
                report_id = response_dict['data']['report_id']
                break

        if not report_id:
            raise TimeoutError('Не удалось получить report_id после 4 попыток')

        ti.xcom_push(key='report_id', value=report_id)
        logging.info(f"По результату запроса report_id={report_id}.")

    except Exception:
        logging.exception('Ошибка при отправке запроса на получение строкового идентификатора "report_id".')
        raise


def get_increment(date, ti):
    """Метод получает строковый идентификатор для дальнейшей выгрузки данных."""

    try:
        logging.info('Отправляем запрос на получение строкового идентификатора "get_increment".')

        method_url = '/get_increment'
        report_id = ti.xcom_pull(key='report_id')
        response = requests.get(f'{api_endpoint}{method_url}?report_id={report_id}&date={str(date)}T00:00:00',
                                headers=headers)
        response.raise_for_status()
        increment_id = json.loads(response.content)['data']['increment_id']

        if not increment_id:
            raise ValueError('Значение increment_id пустое.')

        ti.xcom_push(key='increment_id', value=increment_id)
        logging.info(f"По результату запроса increment_id={increment_id}.")

    except Exception:
        logging.exception('Ошибка при отправке запроса на получение строкового идентификатора "get_increment".')
        raise


def create_local_files(ti, filename, date):
    """Метод создает локальные файлы с данными из s3."""

    try:
        logging.info(f'Отправляем запрос на выгрузку данных из файла {filename}.')

        increment_id = ti.xcom_pull(key='increment_id')
        method_url = f'/{increment_id}/{filename}'
        s3_filename = s3 + method_url

        logging.info(f'Ссылка для файла {filename}: {s3_filename}.')

        response = requests.get(s3_filename)
        response.raise_for_status()

        # Создаем локальные файлы в которые загрузим данные.
        local_filename = f'{date}_{filename}'
        with open(local_filename, "wb") as f:
            f.write(response.content)

        logging.info(f'Локальный файл "{local_filename}" создан.')

    except Exception:
        logging.exception(f'Ошибка при отправке запроса на выгрузку данных из файла {filename}.')
        raise


# Проверка качества данных.
def check_success_customer_research_inc_name(context):
    """Метод дает обратную свзять если файл customer_research_inc существует."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('customer_research_inc_name', 'file_sensor', %s, '0')
        """,
        parameters=(business_dt,)
    )

    logging.info('Локальный файл customer_research_inc существует.')


def check_failure_customer_research_inc_name(context):
    """Метод дает обратную свзять если файл customer_research_inc не существует."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('customer_research_inc_name', 'file_sensor', %s, '1')
        """,
        parameters=(business_dt,)
    )

    logging.info('Локальный файл customer_research_inc не существует.')


def check_success_user_order_log_inc_name(context):
    """Метод дает обратную свзять если файл user_order_log_inc существует."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('user_order_log_inc_name', 'file_sensor', %s, '0')
        """,
        parameters=(business_dt,)
    )

    logging.info('Локальный файл user_order_log_inc существует.')


def check_failure_user_order_log_inc_name(context):
    """Метод дает обратную свзять если файл user_order_log_inc не существует."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('user_order_log_inc_name', 'file_sensor', %s, '1')
        """,
        parameters=(business_dt,)
    )

    logging.info('Локальный файл user_order_log_inc не существует.')


def check_success_user_activity_log_inc_name(context):
    """Метод дает обратную свзять если файл user_activity_log_inc существует."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('user_activity_log_inc_name', 'file_sensor', %s, '0')
        """,
        parameters=(business_dt,)
    )

    logging.info('Локальный файл user_activity_log_inc существует.')


def check_failure_user_activity_log_inc_name(context):
    """Метод дает обратную свзять если файл user_activity_log_inc не существует."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('user_activity_log_inc_name', 'file_sensor', %s, '1')
        """,
        parameters=(business_dt,)
    )

    logging.info('Локальный файл user_activity_log_inc не существует.')


def load_file_to_bd(filename, pg_table, pg_schema, date):
    """Метод производит миграцию данных из локальных файлов в БД."""

    try:
        local_filename = f'{date}_{filename}'

        # Обрабатываем данные в созданном файле.
        df = pd.read_csv(local_filename)
        if filename != 'customer_research_inc.csv':
            df = df.drop('id', axis=1)
            df = df.drop_duplicates(subset=['uniq_id'])

        if 'status' not in df.columns:
            df['status'] = 'shipped'

        logging.info(f'Данные в локальном файле "{local_filename}" обработаны.')

        # Подключаемся к базе данных PostgreSQL и мигрируем данные.
        postgres_hook = PostgresHook(postgres_conn_id)

        # Конвертируем данные в список кортежей.
        rows = [tuple(x) for x in df.to_numpy()]
        row_count = len(rows)

        columns = list(df.columns)

        postgres_hook.insert_rows(
            table=f'{pg_schema}.{pg_table}',
            rows=rows,
            target_fields=columns
        )

        logging.info(f'{row_count} записей было вставлено в таблицу {pg_table}.')

    except Exception:
        logging.exception(f'Ошибка при миграции данных из файла {local_filename}.')
        raise


# Проверка качества данных.
def check_success_user_order_log_isNull(context):
    """Метод дает обратную свзять если в таблице user_order_log
    в поле customer_id нет значения NULL."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('user_order_log', 'user_order_log_isNull', %s, '0')
        """,
        parameters=(business_dt,)
    )

    logging.info('В таблице user_order_log в поле customer_id не присутствует значение NULL.')


def check_failure_user_order_log_isNull(context):
    """Метод дает обратную свзять если в таблице user_order_log
    в поле customer_id есть значение NULL."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('user_order_log', 'user_order_log_isNull', %s, '1')
        """,
        parameters=(business_dt,)
    )

    logging.info('В таблице user_order_log в поле customer_id присутствует значение NULL.')


def check_success_user_activity_log_isNull(context):
    """Метод дает обратную свзять если в таблице user_activity_log
    в поле customer_id нет значения NULL."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('user_activity_log', 'user_activity_log_isNull', %s, '0')
        """,
        parameters=(business_dt,)
    )

    logging.info('В таблице user_activity_log в поле customer_id не присутствует значение NULL.')


def check_failure_user_activity_log_isNull(context):
    """Метод дает обратную свзять если в таблице user_activity_log
    в поле customer_id есть значение NULL."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('user_activity_log', 'user_activity_log_isNull', %s, '1')
        """,
        parameters=(business_dt,)
    )

    logging.info('В таблице user_activity_log в поле customer_id присутствует значение NULL.')


def check_success_user_order_log_count(context):
    """Метод дает обратную свзять если в таблице user_order_log
    число клиентов больше 3-х."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('user_order_log', 'check_row_count_user_order_log', %s, '0')
        """,
        parameters=(business_dt,)
    )

    logging.info('В таблице user_order_log число клиентов больше 3-х.')


def check_failure_user_order_log_count(context):
    """Метод дает обратную свзять если в таблице user_order_log
    число клиентов меньше 3-х."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('user_order_log', 'check_row_count_user_order_log', %s, '1')
        """,
        parameters=(business_dt,)
    )

    logging.info('В таблице user_order_log число клиентов меньше 3-х.')


def check_success_user_activity_log_count(context):
    """Метод дает обратную свзять если в таблице user_activity_log
    число клиентов больше 3-х."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('user_activity_log', 'check_row_count_user_activity_log', %s, '0')
        """,
        parameters=(business_dt,)
        )

    logging.info('В таблице user_activity_log число клиентов больше 3-х.')


def check_failure_user_activity_log_count(context):
    """Метод дает обратную свзять если в таблице user_activity_log
    число клиентов меньше 3-х."""

    business_dt = context['ds']
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        INSERT INTO checks.dq_checks_results
        VALUES ('user_activity_log', 'check_row_count_user_activity_log', %s, '1')
        """,
        parameters=(business_dt,)
        )

    logging.info('В таблице user_activity_log число клиентов меньше 3-х.')
