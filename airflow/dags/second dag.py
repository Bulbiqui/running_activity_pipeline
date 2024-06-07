from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

import endpoints
from env_handler import env_variables

# Configuration du DAG

default_args = {
    'owner': 'airflow',
    'retries': 1
}

dag = DAG(
    'fetch_and_save_activity_data',
    default_args=default_args,
    description= 'Fetch activity data and save to CSV',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
)

# used to f.e set the limit of fetched activities (default - 30)
ACTIVITIES_PER_PAGE = 200
# current page number with activities
PAGE_NUMBER = 1

GET_ALL_ACTIVITIES_PARAMS = {
    'per_page': ACTIVITIES_PER_PAGE,
    'page': PAGE_NUMBER
}

def get_access_token():
    # these params needs to be passed to get access
    # token used for retrieveing actual data
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

def access_activity_data(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='get_access_token')
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(endpoints.activites_endpoint, headers=headers, params=GET_ALL_ACTIVITIES_PARAMS)
    response.raise_for_status()
    activity_data = response.json()
    ti.xcom_push(key='activity_data', value=activity_data)

def preprocess_and_save_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='access_activity_data', key='activity_data')
    df = pd.json_normalize(data)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = Path('../../../',f'my_activity_data_{timestamp}.csv').resolve()
    df.to_csv(output_path, index=False)
    print(output_path)

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

get_access_token_task >> access_activity_data_task >> preprocess_and_save_data_task