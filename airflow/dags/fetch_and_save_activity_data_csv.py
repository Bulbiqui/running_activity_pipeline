from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from io import StringIO
from airflow.hooks.S3_hook import S3Hook

import endpoints
from env_handler import env_variables

# Configuration du DAG

default_args = {
    'owner': 'airflow',
    'retries': 1
}

dag = DAG(
    'fetch_and_save_activity_data_csv',
    default_args=default_args,
    description= 'Fetch activity data and save to CSV',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
)

# Nombre d'activité à récupérer
ACTIVITIES_PER_PAGE = 300
PAGE_NUMBER = 1

GET_ALL_ACTIVITIES_PARAMS = {
    'per_page': ACTIVITIES_PER_PAGE,
    'page': PAGE_NUMBER
}

# Récupération du jeton d'accès à l'api strava
def get_access_token():
    payload:dict = {
    'client_id': env_variables['CLIENT_ID'],
    'client_secret': env_variables['CLIENT_SECRET'],
    'refresh_token': env_variables['REFRESH_TOKEN'],
    'grant_type': "refresh_token",
    'f': 'json'
    }
    res = requests.post(endpoints.auth_endpoint, data=payload, verify=False)
    res.raise_for_status()
    access_token = res.json()['access_token']
    return access_token

# Récupération des données d'activité au format json
def access_activity_data(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='get_access_token')
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(endpoints.activites_endpoint, headers=headers, params=GET_ALL_ACTIVITIES_PARAMS)
    response.raise_for_status()
    activity_data = response.json()
    ti.xcom_push(key='activity_data', value=activity_data)

# Transformation du fichier json en csv
def preprocess_and_save_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='access_activity_data', key='activity_data')
    df = pd.json_normalize(data)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f'my_activity_data_{timestamp}.csv'
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    ti.xcom_push(key='file_name', value=file_name)
    ti.xcom_push(key='csv_data', value=csv_buffer.getvalue())
    print(f'Data prepared to be saved as {file_name}')

# Upload du fichier csv dans le s3 bucket
def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids='preprocess_and_save_data', key='file_name')
    csv_data = ti.xcom_pull(task_ids='preprocess_and_save_data', key='csv_data')
    s3_bucket = 'running-activity-aog'
    s3_key = f'{file_name}'

    s3_hook = S3Hook(aws_conn_id='S3amazon')
    s3_hook.load_string(string_data=csv_data, key=s3_key, bucket_name=s3_bucket, replace=True)
    print(f'File {file_name} uploaded to S3 bucket {s3_bucket} with key {s3_key}')


with dag:
    get_access_token_task = PythonOperator(
        task_id='get_access_token',
        python_callable=get_access_token,
    )

    access_activity_data_task = PythonOperator(
        task_id='access_activity_data',
        python_callable=access_activity_data,
        provide_context=True,
    )

    preprocess_and_save_data_task = PythonOperator(
        task_id='preprocess_and_save_data',
        python_callable=preprocess_and_save_data,
        provide_context=True,
    )

    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        provide_context=True,
    )

get_access_token_task >> access_activity_data_task >> preprocess_and_save_data_task >> upload_to_s3_task