from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc  import DataprocCreateClusterOperator,DataprocDeleteClusterOperator,DataprocSubmitJobOperator
 





# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dataproc_pyspark_hello_world',
    default_args=default_args,
    description='A DAG to run PySpark Hello World on Dataproc',
    schedule_interval='*/10 * * * *',  # Adjust as per your requirements
)

# Define cluster creation task
create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    cluster_name='dataproc-cluster-{{ ds_nodash }}',
    project_id='ajith-poc',
    num_workers=2,  # Number of worker nodes in the cluster
    region = 'asia-south2',
    zone = 'asia-south2-a',
    dag=dag,
)

pyspark_task = DataprocSubmitJobOperator(
    task_id='run_pyspark_job',
    project_id='ajith-poc',
    region='asia-south2',    
    job={
        'reference': {
            'project_id': 'ajith-poc',
            'job_id': 'dataproc-pyspark-hello-world'
        },
        'placement': {
            'cluster_name': 'dataproc-cluster-{{ ds_nodash }}'
        },
        'pyspark_job': {
            'gs://bucket-airflow-one/airflow.py',
            # You can add other properties like args, properties, etc. as needed
        }
    },
    dag=dag,
)



# Define cluster deletion task
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    cluster_name='dataproc-cluster-{{ ds_nodash }}',
    project_id='ajith-poc',
    region='asia-south2',
    trigger_rule='all_done',
    dag=dag,
)

# Define task dependencies
create_cluster >> pyspark_task >> delete_cluster
