from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1),
}

# Create a dummy file for attachment
BASE_LOCATION = '/opt'
os.makedirs(f'{BASE_LOCATION}/airflow/test', exist_ok=True)
with open(f'{BASE_LOCATION}/airflow/test/test_attachment.txt', 'w') as f:
    f.write("This is a test attachment for the email.")

# Define the DAG
with DAG(
    dag_id='test_email',
    default_args=default_args,
    description='A simple DAG to send an email using smtp_default',
    schedule_interval='@once',
    catchup=False,
    tags=['email', 'test'],
) as dag:

    # Task: Send test email
    send_email = EmailOperator(
        task_id='send_test_email',
        to='dungngo0935431740@gmail.com',
        subject='Test Email from Airflow (Gmail SMTP)',
        html_content="""
        <h3>Hello from Airflow!</h3>
        <p>This is a test email sent using the smtp_default connection (smtp.gmail.com, port 587).</p>
        <p>Please check the attached file.</p>
        """,
        files=[f'{BASE_LOCATION}/airflow/test/test_attachment.txt'],
        conn_id='smtp_default',
        mime_charset='utf-8',
    )

    # Task sequence (single task)
    send_email