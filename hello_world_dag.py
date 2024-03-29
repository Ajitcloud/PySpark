from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)
 





# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG
dag = DAG(
    'dataproc_pyspark_hello_world',
    default_args=default_args,
    description='A DAG to run PySpark Hello World on Dataproc',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2023, 1, 1),  # Adjust as per your requirements
)

# Define cluster creation task
create_cluster = DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    cluster_name='spark-cluster-{{ ds_nodash }}',
    project_id='ajith-poc',
    num_workers=2,  # Number of worker nodes in the cluster
    region = 'asia-south2',
    zone = 'asia-south2-a',
    dag=dag,
)

pyspark_task=DataProcPySparkOperator(
        task_id='pyspark_task',
        region = 'asia-south2',
        main='gs://bucket-airflow-one/airflow.py',
        arguments=[' ',' '],
        cluster_name="spark-cluster-{{ ds_nodash }}",
     )



# Define cluster deletion task
delete_cluster = DataprocClusterDeleteOperator(
    task_id='delete_dataproc_cluster',
    cluster_name='spark-cluster-{{ ds_nodash }}',
    project_id='ajith-poc',
    region='asia-south2',
    trigger_rule='all_done',
    dag=dag,
)

# Define task dependencies
create_cluster >> pyspark_task >> delete_cluster

