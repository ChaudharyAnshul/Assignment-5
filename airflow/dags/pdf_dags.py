import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from dags.scripts.extraction_pypdf import process_pdf
from dags.scripts.validate import cleanDataPDF
from dags.scripts.load_data import loadData

dag = DAG(
    dag_id="pdf_dag",
    schedule=None,
    start_date=days_ago(0),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

def extract_data(**kwargs):
    s3_uri = kwargs['dag_run'].conf.get("s3_uri")
    csv_s3_uri = process_pdf(s3_uri)
    kwargs['ti'].xcom_push(key='raw_data_pdf', value=csv_s3_uri)

def validate_data(**kwargs):
    pulled_value = kwargs['ti'].xcom_pull(dag_id='pdf_dag', task_ids='extract_data', key='raw_data_pdf')
    file_name = cleanDataPDF(pulled_value)
    kwargs['ti'].xcom_push(key='clean_data_pdf', value=file_name)
    

def load_data(**kwargs):
    pulled_value = kwargs['ti'].xcom_pull(dag_id='pdf_dag', task_ids='validate_data', key='clean_data_pdf')
    loadData(pulled_value)

with dag:
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
        dag=dag,
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
        dag=dag,
    )
    
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
        dag=dag,
    )

    extract_data_task >> validate_data_task >> load_data_task