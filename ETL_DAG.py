from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.sql import SQLCheckOperator, SQLValueCheckOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from Script import (
    generate_report,
    get_report,
    get_increment,
    create_local_files,
    check_success_customer_research_inc_name,
    check_failure_customer_research_inc_name,
    check_success_user_order_log_inc_name,
    check_failure_user_order_log_inc_name,
    check_success_user_activity_log_inc_name,
    check_failure_user_activity_log_inc_name,
    load_file_to_bd,
    check_success_user_order_log_isNull,
    check_failure_user_order_log_isNull,
    check_success_user_activity_log_isNull,
    check_failure_user_activity_log_isNull,
    check_success_user_order_log_count,
    check_failure_user_order_log_count,
    check_success_user_activity_log_count,
    check_failure_user_activity_log_count,
    postgres_conn_id,
    file_search_local,
)

business_dt = '{{ ds }}'  # Текущая дата.

args = {
    'owner': 'stmax22',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
        dag_id='uploading_data_in_week',
        default_args=args,
        description='Uploading data for the past week',
        catchup=True,
        start_date=datetime.today() - timedelta(days=7),
        end_date=datetime.today() - timedelta(days=1),
) as dag:

    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report
        )

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report
        )

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt}
        )

    with TaskGroup(group_id='create_local_files') as group_create_files:
        create_customer_research_inc = PythonOperator(
            task_id='create_customer_research_inc',
            python_callable=create_local_files,
            op_kwargs={
                'date': business_dt,
                'filename': 'customer_research_inc.csv'
                }
            )

        create_user_order_log_inc = PythonOperator(
            task_id='create_user_order_log_inc',
            python_callable=create_local_files,
            op_kwargs={
                'date': business_dt,
                'filename': 'user_order_log_inc.csv'
                }
            )

        create_user_activity_log_inc = PythonOperator(
            task_id='create_user_activity_log_inc',
            python_callable=create_local_files,
            op_kwargs={
                'date': business_dt,
                'filename': 'user_activity_log_inc.csv'
                }
            )

    with TaskGroup(group_id='check_files_name') as group_check_files_name:
        f1 = FileSensor(
            task_id='check_name_customer_research',
            fs_conn_id=file_search_local,
            filepath=f'{business_dt}_customer_research_inc.csv',
            poke_interval=5,
            on_success_callback=check_success_customer_research_inc_name,
            on_failure_callback=check_failure_customer_research_inc_name
            )

        f2 = FileSensor(
            task_id='check_name_user_activity_log',
            fs_conn_id=file_search_local,
            filepath=f'{business_dt}_user_activity_log_inc.csv',
            poke_interval=5,
            on_success_callback=check_success_user_activity_log_inc_name,
            on_failure_callback=check_failure_user_activity_log_inc_name
            )

        f3 = FileSensor(
            task_id='check_name_user_order_log',
            fs_conn_id=file_search_local,
            filepath=f'{business_dt}_user_order_log_inc.csv',
            poke_interval=5,
            on_success_callback=check_success_user_order_log_inc_name,
            on_failure_callback=check_failure_user_order_log_inc_name
            )

    with TaskGroup(group_id='load_files_to_bd') as group_load_files_to_bd:
        load_customer_research_inc = PythonOperator(
            task_id='load_customer_research_inc',
            python_callable=load_file_to_bd,
            op_kwargs={
                'date': business_dt,
                'filename': 'customer_research_inc.csv',
                'pg_table': 'customer_research',
                'pg_schema': 'stage'
                }
            )

        load_user_order_log_inc = PythonOperator(
            task_id='load_user_order_log_inc',
            python_callable=load_file_to_bd,
            op_kwargs={
                'date': business_dt,
                'filename': 'user_order_log_inc.csv',
                'pg_table': 'user_order_log',
                'pg_schema': 'stage'
                }
            )

        load_user_activity_log_inc = PythonOperator(
            task_id='load_user_activity_log_inc',
            python_callable=load_file_to_bd,
            op_kwargs={
                'date': business_dt,
                'filename': 'user_activity_log_inc.csv',
                'pg_table': 'user_activity_log',
                'pg_schema': 'stage'
                }
            )

    with TaskGroup(group_id='sql_check') as group_sql_check:
        sql_check = SQLValueCheckOperator(
            task_id='user_order_log_isNull',
            conn_id=postgres_conn_id,
            sql='sql/check_user_order_log_isNull.sql',
            pass_value=0,
            on_success_callback=check_success_user_order_log_isNull,
            on_failure_callback=check_failure_user_order_log_isNull
            )

        sql_check2 = SQLValueCheckOperator(
            task_id='user_activity_log_isNull',
            conn_id=postgres_conn_id,
            sql='sql/check_user_activity_log_isNull.sql',
            pass_value=0,
            on_success_callback=check_success_user_activity_log_isNull,
            on_failure_callback=check_failure_user_activity_log_isNull
            )

        sql_check3 = SQLCheckOperator(
            task_id='check_row_count_user_order_log',
            conn_id=postgres_conn_id,
            sql='sql/check_row_count_user_order_log.sql',
            on_success_callback=check_success_user_order_log_count,
            on_failure_callback=check_failure_user_order_log_count
            )

        sql_check4 = SQLCheckOperator(
            task_id='check_row_count_user_activity_log',
            conn_id=postgres_conn_id,
            sql='sql/check_row_count_user_activity_log.sql',
            on_success_callback=check_success_user_activity_log_count,
            on_failure_callback=check_failure_user_activity_log_count
            )

    with TaskGroup(group_id='update_data_mart_d') as update_d:
        update_d_calendar = PostgresOperator(
            task_id='update_d_calendar',
            postgres_conn_id=postgres_conn_id,
            sql='sql/mart.d_calendar.sql',
            dag=dag
            )

        update_d_city = PostgresOperator(
            task_id='update_d_city',
            postgres_conn_id=postgres_conn_id,
            sql='sql/mart.d_city.sql',
            dag=dag
            )

        update_d_customer = PostgresOperator(
            task_id='update_d_customer',
            postgres_conn_id=postgres_conn_id,
            sql='sql/mart.d_customer.sql',
            dag=dag
            )

        update_d_item = PostgresOperator(
            task_id='update_d_item',
            postgres_conn_id=postgres_conn_id,
            sql='sql/mart.d_item.sql',
            dag=dag
            )

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql='sql/mart.f_sales.sql'
        )

    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql='sql/mart.f_customer_retention.sql'
        )

    (
        generate_report
        >> get_report
        >> get_increment
        >> group_create_files
        >> group_check_files_name
        >> group_load_files_to_bd
        >> group_sql_check
        >> update_d
        >> update_f_sales
        >> update_f_customer_retention
    )
