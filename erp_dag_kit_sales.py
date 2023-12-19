import datetime as dt

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.sensors.external_task import ExternalTaskSensor


default_args = {
    'owner': 'Швейников Андрей',
    'email': ['xxxRichiexxx@yandex.ru'],
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'erp_and_isc_dm',
        default_args=default_args,
        description='Продажи комплектов. Суммарные объемы из ERP и ИСК.',
        start_date=dt.datetime(2023, 12, 15),
        schedule_interval='@daily',
        catchup=False,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Ожидание_формирования_слоя_DDS') as dds_wait:

        wait_dags = (
            'isc_dealer_sales',
            'erp_kit_sales',
            'isc_classifier',
            'isc_realization',
        )
        tasks = []

        for wait_dag in wait_dags:
            tasks.append(
                ExternalTaskSensor(
                    task_id=f'{dag}_wait',
                    external_dag_id=dag,
                    external_task_id='Конец',
                )
            )

    with TaskGroup('Загрузка_данных_в_dm_слой') as data_to_dm:

        dm_TEST_erp_sales = VerticaOperator(
                    task_id='dm_TEST_erp_sales',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_TEST_erp_sales.sql',
                )

        dm_TEST_isc_sales = VerticaOperator(
                    task_id='dm_TEST_isc_sales',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_TEST_isc_sales.sql',
                )

        dm_TEST_isc_balance = VerticaOperator(
                    task_id='dm_TEST_isc_balance',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_TEST_isc_balance.sql',
                )

        [dm_TEST_erp_sales, dm_TEST_isc_sales, dm_TEST_isc_balance]

    with TaskGroup('Проверки') as data_checks:

        dm_TEST_erp_sales_check = VerticaOperator(
                    task_id='dm_TEST_erp_sales_check',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_TEST_erp_sales_check.sql',
                    params={
                        'dm': 'dm_TEST_erp_sales',
                    }
                )
    
        dm_TEST_erp_sales_check

    end = DummyOperator(task_id='Конец')

    start >> dds_wait >> data_to_dm >> data_checks >> end
