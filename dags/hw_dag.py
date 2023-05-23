import datetime as dt
import os
import sys

# Определите путь до вашего проекта
path = '/home/airflow/airflow_hw'

# Добавьте этот путь к системному пути
sys.path.insert(0, path)

from airflow.models import DAG
from airflow.operators.python import PythonOperator

# Импортируйте ваши модули после добавления пути в sys.path
from modules.pipeline import pipeline
from modules.predict import predict as predict_fn

# Добавьте путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path


args = {
    'owner': 'airflow',
    'start_date': dt.datetime.now(),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}


with DAG(
        dag_id='car_price_prediction',
        schedule="00 15 * * *",
        default_args=args,
) as dag:
    pipeline_task = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
    )

    predict_task = PythonOperator(
        task_id='predict',
        python_callable=predict_fn,
    )


    pipeline_task >> predict_task