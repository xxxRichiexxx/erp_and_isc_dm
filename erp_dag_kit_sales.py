
import sqlalchemy as sa
from urllib.parse import quote
import datetime as dt
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.sensors.external_task import ExternalTaskSensor


dwh_con = BaseHook.get_connection('vertica')
ps = quote(dwh_con.password)
dwh_engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_con.login}:{ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
)


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
        start_date=dt.datetime(2023, 9, 1),
        schedule_interval='@daily',
        catchup=False,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Ожидание формирования слоя DDS') as dds_wait:

        isc_wait = ExternalTaskSensor(
            task_id='isc_wait',
            external_dag_id='isc_dealer_sales',
            external_task_id='Конец',
        )

        erp_wait = ExternalTaskSensor(
            task_id='erp_wait',
            external_dag_id='erp_kit_sales',
            external_task_id='Конец',
        )

        [isc_wait, erp_wait]


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

        pass

    end = DummyOperator(task_id='Конец')

    start >> dds_wait >> data_to_dm >> data_checks >> end
