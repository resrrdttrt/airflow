from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Create the DAG
with DAG(
    dag_id='test_docker',
    default_args=default_args,
    schedule_interval='@once',  # Run once
    catchup=False,
) as dag:

    # Task to run the Docker container
    run_container = DockerOperator(
        task_id='run_python_script',
        image='list-files-app',  # The name of your Docker image
        container_name='list_files_container',
        api_version='auto',  # Auto-detect Docker API version
        auto_remove=True,  # Automatically remove the container when it exits
        environment={
            'FILE_NAME': 'test.txt',  # Set your environment variable
        },
        mounts=[
            {
                'source': '/home/vandung545917/airflow/test/testFolder',
                'target': '/app/testFolder',
                'type': 'bind',  # Use 'bind' for local directories
            },
        ],
        command='python app.py',  # Command to run in the container
    )

    # Define the task sequence
    run_container