from datetime import datetime, timedelta, timezone
import requests

import pandas as pd
from airflow.sdk import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.decorators import task
from airflow.providers.mongo.sensors.mongo import MongoHook

from app.loging_etl import logger

# Получаем API-ключ из переменных Airflow
API_KEY = Variable.get("data_nasa_api")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2025-05-13',
    'email_on_failure': True,
    'email': ['georgijmironenko36@gmail.com'],
    'retries': 4,
    'retry_delay': timedelta(minutes=2),
}

# Создаём и настраиваем dag
with DAG(
    dag_id='nasa_data_pipeline',
    default_args=default_args,
    schedule='50 23 * * *',
    catchup=False
) as dag:

    @task
    def extract():
        # вчерашняя дата в UTC
        query_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')
        # Создаём url вчерашнего дня
        url = f"https://api.nasa.gov/EPIC/api/natural/date/{query_date}?api_key={API_KEY}"

        try:
            response = requests.get(url)

            if response.status_code == 200:
                data_json = response.json()
                logger.debug("Успешное преобразование в json")

                rows = []
                if data_json:
                    for i in data_json:
                        row = (
                            str(i["date"]),
                            round(i["dscovr_j2000_position"]["x"], 6),
                            round(i["dscovr_j2000_position"]["y"], 6),
                            round(i["dscovr_j2000_position"]["z"], 6),
                            round(i["lunar_j2000_position"]["x"], 6),
                            round(i["lunar_j2000_position"]["y"], 6),
                            round(i["lunar_j2000_position"]["z"], 6),
                            round(i["sun_j2000_position"]["x"], 6),
                            round(i["sun_j2000_position"]["y"], 6),
                            round(i["sun_j2000_position"]["z"], 6),
                            i["image"]
                        )
                        rows.append(row)

                    df = pd.DataFrame(rows, columns=[
                        'date', 'x', 'y', 'z',
                        'x_lunar', 'y_lunar', 'z_lunar',
                        'x_sun', 'y_sun', 'z_sun',
                        'image'
                    ])

                    # Создаём dataframe для MongoDB

                    # Удаляем колонку image
                    df_mongo = df.drop(columns=['image'])
                    logger.debug("Удалили колонку image")

                    # Приводим к дате (без времени)
                    df_mongo['date'] = pd.to_datetime(df_mongo['date']).dt.date
                    logger.debug("Привели к нужному формату даты.")

                    # Группируем по дате и считаем среднее
                    df_mongo = df_mongo.groupby('date').mean(numeric_only=True).reset_index()
                    logger.debug("Сгрупировали и сочитали среднее")

                    # Приводим дату к строке для MongoDB
                    df_mongo['date'] = df_mongo['date'].astype(str)
                    logger.debug("Привели дату к строке для MongoDB")

                    # Готовим к возврату
                    return {
                        "postgres": df.to_dict(orient="records"), # Данные без изменений
                        "mongo": df_mongo.to_dict(orient="records"),  # Среднее по дню без image
                    }

                else:
                    logger.error(f"Данных за {query_date} пока нет.")
                    raise AirflowSkipException("Данных пока нет. Пропуск.")
            else:
                logger.error("Ошибка при запросе к API")
                raise Exception(f"Ошибка при запросе к API: {response.status_code}")

        except Exception as error:
            logger.error(f"Ошибка при обработке данных: {error}")
            raise


    @task
    def load_to_postgres(data: dict):
        records = data.get("postgres", [])
        hook = PostgresHook(postgres_conn_id='data_nasa_base')
        conn = hook.get_conn()
        cursor = conn.cursor()

        # SQL-запрос для создания таблицы, если она не существует
        create_table_query = """
        CREATE TABLE IF NOT EXISTS public.nasa_epic_data (
            date VARCHAR(50),
            x NUMERIC,
            y NUMERIC,
            z NUMERIC,
            x_lunar NUMERIC,
            y_lunar NUMERIC,
            z_lunar NUMERIC,
            x_sun NUMERIC,
            y_sun NUMERIC,
            z_sun NUMERIC,
            image VARCHAR(255)
        );
        """

        # Выполнение запроса на создание таблицы
        cursor.execute(create_table_query)
        logger.debug("У спешно создали таблицы в бд")

        # SQL-запрос для вставки данных
        insert_sql = """
        INSERT INTO public.nasa_epic_data (
            date, x, y, z, x_lunar, y_lunar, z_lunar, x_sun, y_sun, z_sun, image
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Вставка данных
        for record in records:
            cursor.execute(insert_sql, (
                record['date'],
                record['x'], record['y'], record['z'],
                record['x_lunar'], record['y_lunar'], record['z_lunar'],
                record['x_sun'], record['y_sun'], record['z_sun'],
                record['image']
            ))
        logger.debug("Успешно вставили данные")

        # Сохранение изменений
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Успешное сохранение в posgresql")


    @task
    def load_to_mongodb(data: dict):
        records = data.get("mongo", [])
        if not records:
            logger.error("Нет данных для загрузки в MongoDB")
            return

        try:
            hook = MongoHook(mongo_conn_id='mongo_my')
            client = hook.get_conn()
            db = client['airflow']
            collection = db['nasa_data']
            result = collection.insert_many(records)
            logger.info(f"Загружено записей в MongoDB: {len(result.inserted_ids)}")

        except Exception as e:
            logger.error(f"Ошибка при записи в MongoDB: {str(e)}")
            raise

    # Выполняем задачи
    extracted_data = extract()
    load_to_postgres(extracted_data)
    load_to_mongodb(extracted_data)