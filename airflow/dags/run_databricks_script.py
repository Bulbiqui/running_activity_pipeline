from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator, DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'run_databricks_notebook',
    default_args=default_args,
    description='Run Databricks notebook',
    schedule_interval=None,  # Définissez votre propre planification ou déclenchez manuellement
    start_date=days_ago(1),
    catchup=False,
)

with dag:

    notebook_task_params = {
        'new_cluster': '0706-095644-tjclpwv9',
        'notebook_task': {
            'notebook_path': '/Users/ogez.antoine@yahoo.fr/import_and_transformation',
        },
    }
    # [START howto_operator_databricks_json]
    # Example of using the JSON parameter to initialize the operator.
    notebook_task = DatabricksSubmitRunOperator(
        task_id='notebook_task',
        databricks_conn_id = 'Databricks_connection',
        json=notebook_task_params)


notebook_task