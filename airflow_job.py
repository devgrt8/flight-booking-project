from datetime import datetime, timedelta
import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'start_date': datetime(2026,7,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'flight_booking_dataproc_bq_job',
    default_args=default_args,
    description='A DAG to submit a Dataproc job',
    schedule_interval=None,
    catchup=False
) as dag:
    
    #Fetch Environment Variables
    env = Variable.get("env", default_var="dev")
    gcs_bucket = Variable.get("gcs_bucket", default_var="flight-booking-analysis-99")
    bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")
    bq_project = Variable.get("bq_project", default_var="project-b2037a87-f225-4144-a0b")
    #fetch tables to be processed from Airflow Variables
    tables = Variable.get("tables", deserialize_json=True)
    
    transformed_table = tables['transformed_table']
    route_insights_table = tables['route_insights_table']
    origin_insights_table = tables['origin_insights_table']

    job_batch_id = f"flight_booking_batch_{env}_{uuid.uuid4().hex[:8]}"


    # Task 1 - Check for the existence of the file in GCS
    file_sensor = GCSObjectExistenceSensor(
        task_id='check_file_existence',
        bucket=gcs_bucket,
        object=f'{gcs_bucket}/source_{env}/*.csv',
        gcp_conn_id='google_cloud_default',
        poke_interval=60,  # Check every 60 seconds
        timeout=600  # Timeout after 10 minutes
        mode='poke' # poke mode means it will keep running until the condition is met or timeout occurs
    )


    batch_detail ={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{gcs_bucket}/dataproc_jobs/flight_booking_job.py",
                "args": [
                    f"--env={env}",
                    f"--gcs_bucket={gcs_bucket}",
                    f"--bq_dataset={bq_dataset}",
                    f"--transformed_table={transformed_table}",
                    f"--route_insights_table={route_insights_table}",
                    f"--origin_insights_table={origin_insights_table}"
                ],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.29.0.jar"
                ],
            },
               "execution_config": {
                    "service_account": "1025869045610-compute@developer.gserviceaccount.com",
                    "network_uri": f"projects/{bq_project}/global/networks/default",
                    "subnetwork_uri": f"projects/{bq_project}/regions/us-central1/subnetworks/default",
            }
        }

    pyspark_job = DataprocCreateBatchOperator(
        task_id='submit_pyspark_job',
        batch= batch_detail,
        batch_id=job_batch_id,
        gcp_conn_id='google_cloud_default'
        project_id=bq_project,
        region='us-central1',
        gcp_conn_id='google_cloud_default',
    )


file_sensor >> pyspark_job