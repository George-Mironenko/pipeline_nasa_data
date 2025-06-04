from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Получение API ключа из переменных Airflow
API_KEY = Variable.get("nasa_api")

# Создание DAG
with DAG(
    dag_id='clichouse',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 6, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule='@daily',
    catchup=False,
) as dag:

    @task()
    def extract_transform_data():
        # Это будет задача для извлечения и преобразования данных
        return 0


    @task()
    def load_clickhouse_data():
        # Это задача для загрузки данных в ClickHouse
        pass

    @task()
    def load_data_postrege():
        # Это задача для загрузки данных в PostgreSQL
        pass

    # Запуск DAG
    extracted_data = extract_transform_data()
    load_clickhouse_data(extracted_data)
    load_data_postrege(extracted_data)
