import csv
from datetime import datetime, timedelta
from io import StringIO

import boto3
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG


default_args = {
    'owner': 'airflow2',
    'depends_on_past': False,
    'email_on_failure': True,
    'start_date': datetime(2024, 12, 31),
    'email': ['georgijmironenko36@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='postgresql_s3',
    schedule='59 23 31 12 *',
    default_args=default_args,
    catchup=False,
    fail_fast=True
) as dag:

    @task
    def extract_postgres():
        try:
            # Подключаемся к базе данных
            hook = PostgresHook(postgres_conn_id='data_nasa_base')
            conn = hook.get_conn()
            cursor = conn.cursor()

            # Получаем данные из базы данных
            cursor.execute("SELECT * FROM public.nasa_epic_data;")
            data = cursor.fetchall()

            # Очищаем таблицу для следующего года
            cursor.execute("DELETE FROM public.nasa_epic_data;")

            conn.commit()
            cursor.close()
            conn.close()

            return data
        except Exception as e:
            print(f"An error occurred: {e}")
            raise

    @task
    def load_s3(data: dict):
        try:
            # Преобразование данных в CSV формат
            output = StringIO()
            csv_writer = csv.writer(output)
            csv_writer.writerows(data)
            csv_data = output.getvalue()

            # Конфигурация для подключения к Selectel Object Storage
            s3 = boto3.client(
                's3',
                endpoint_url='https://s3.storage.selcloud.ru',
                aws_access_key_id=Variable.get("SELECTEL_ACCESS_KEY"),
                aws_secret_access_key=Variable.get("SELECTEL_SECRET_KEY")
            )
            bucket_name = 'nasa'
            file_name = f'data___nasa_{datetime.now().year}.csv'

            # Загрузка данных в Selectel Object Storage
            s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_data)
        except Exception as e:
            print(f"An error occurred: {e}")
            raise


    # Вызываем задачу и присваиваем её переменной
    task_load = extract_postgres()
    load_s3(task_load)
