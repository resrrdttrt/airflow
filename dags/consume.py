from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from confluent_kafka import Consumer, KafkaException

def consume_messages(broker_url, topic_name, group_id):
    # Create a Kafka Consumer
    consumer = Consumer({
        'bootstrap.servers': broker_url,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'  # Start reading from earliest available message
    })

    # Subscribe to the topic
    consumer.subscribe([topic_name])

    print(f"Consuming messages from topic '{topic_name}'...")

    try:
        for _ in range(10):  # Limit to consuming 10 messages for this example
            # Poll for messages from Kafka
            message = consumer.poll(1.0)  # Wait up to 1 second for a message
            
            if message is None:
                print("1")
                continue
            if message.error():
                print("2")
                raise KafkaException(message.error())
            else:
                # Successfully received a message
                print(f"Received message: {message.value().decode('utf-8')}")

    except KeyboardInterrupt:
        print("Consumption interrupted by user.")

    finally:
        # Close down the consumer to commit final offsets and clean up
        consumer.close()

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'kafka_consumer_dag',
    default_args=default_args,
    description='A DAG to consume messages from Kafka',
    schedule_interval='@once',  # Run once
)

# Create a task to consume messages
consume_task = PythonOperator(
    task_id='consume_kafka_messages',
    python_callable=consume_messages,
    op_kwargs={
        'broker_url': '172.17.0.1:9092',
        'topic_name': 'from_mysql_transaction',
        'group_id': 'test-consumer-group-2343',
    },
    dag=dag,
)

consume_task



