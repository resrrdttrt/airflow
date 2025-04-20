from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Consumer, KafkaError
import json
import psycopg2
import logging
from airflow.utils.dates import days_ago
import uuid

# Configuration
broker_url = '172.17.0.1:9092'
transaction_topic = 'from_postgres_transaction'
amount_topic = 'from_postgres_amount'
db_config = {
    'host': '172.17.0.1',
    'port': '5433',
    'database': 'postgres',
    'user': 'airflow',
    'password': 'airflow'
}
STORE_ID = 'STORE_POSTGRES'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_kafka_transaction_messages():
    logger.info("Starting Kafka consumer for transaction messages...")
    
    consumer_config = {
        'bootstrap.servers': broker_url,
        'group.id': 'official_pg_tran',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([transaction_topic])
    
    logger.info(f"Connected to Kafka broker at {broker_url}")

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    logger.info(f"Connected to PostgreSQL database at {db_config['host']}:{db_config['port']}")

    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS transaction (
            id UUID PRIMARY KEY,
            item_id VARCHAR(50),
            amount INTEGER,
            buy_time TIMESTAMP,
            customer VARCHAR(100),
            store_id VARCHAR(50)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Transaction table created/verified")

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
            else: 
                try:
                    message_value = json.loads(msg.value().decode('utf-8'))
                    logger.info("-------------------")
                    logger.info(f"Processing message from offset: {msg.offset()}")
                    
                    payload = message_value['payload']
                    
                    if payload.get('after'):
                        data = payload['after']
                        data['id'] = str(uuid.uuid4())
                        buy_time = datetime.fromtimestamp(data['buy_time'] / 1000000)
                        insert_data = {
                            'id': data['id'],
                            'item_id': data['item_id'],
                            'amount': data['amount'],
                            'buy_time': buy_time.isoformat(),
                            'customer': data['customer'],
                            'store_id': STORE_ID
                        }
                        # logger.info(f"Inserting transaction data: {json.dumps(insert_data, indent=2)}")
                        
                        # insert_query = """
                        # INSERT INTO transaction (id, item_id, amount, buy_time, customer, store_id)
                        # VALUES (%s, %s, %s, %s, %s, %s)
                        # ON CONFLICT (id) DO UPDATE SET
                        #     item_id = EXCLUDED.item_id,
                        #     amount = EXCLUDED.amount,
                        #     buy_time = EXCLUDED.buy_time,
                        #     customer = EXCLUDED.customer,
                        #     store_id = EXCLUDED.store_id;
                        # """

                        insert_query = """
                        INSERT INTO transaction (id, item_id, amount, customer, store_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            item_id = EXCLUDED.item_id,
                            amount = EXCLUDED.amount,
                            customer = EXCLUDED.customer,
                            store_id = EXCLUDED.store_id;
                        """
                        
                        cursor.execute(insert_query, (
                            data['id'],
                            data['item_id'],
                            data['amount'],
                            # buy_time,
                            data['customer'],
                            STORE_ID
                        ))
                        conn.commit()
                        logger.info(f"Successfully processed transaction for ID: {data['id']}")

                except Exception as e:
                    logger.error(f"Error processing transaction message: {str(e)}")
                    continue

    except Exception as e:
        logger.error(f"Fatal error in transaction consumer: {str(e)}")
        raise e

    finally:
        logger.info("Closing connections for transaction consumer...")
        cursor.close()
        conn.close()
        consumer.close()

def process_kafka_amount_messages():
    logger.info("Starting Kafka consumer for amount messages...")
    
    consumer_config = {
        'bootstrap.servers': broker_url,
        'group.id': 'official_pg_am',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([amount_topic])
    
    logger.info(f"Connected to Kafka broker at {broker_url}")

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    logger.info(f"Connected to PostgreSQL database at {db_config['host']}:{db_config['port']}")

    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS amount (
            id UUID PRIMARY KEY,
            item_id VARCHAR(50),
            amount INTEGER,
            import_time TIMESTAMP,
            store_id VARCHAR(50)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Amount table created/verified")

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
            else: 
                try:
                    message_value = json.loads(msg.value().decode('utf-8'))
                    payload = message_value['payload']
                    if payload.get('after'):
                        data = payload['after']
                        data['id'] = str(uuid.uuid4())
                        import_time = datetime.fromtimestamp(data['import_time'] / 1000000)
                        insert_data = {
                            'id': data['id'],
                            'item_id': data['item_id'],
                            'amount': data['amount'],
                            'import_time': import_time.isoformat(),
                            'store_id': STORE_ID
                        }
                        # logger.info(f"Inserting amount data: {json.dumps(insert_data, indent=2)}")
                        
                        insert_query = """
                        INSERT INTO amount (id, item_id, amount, import_time, store_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            item_id = EXCLUDED.item_id,
                            amount = EXCLUDED.amount,
                            import_time = EXCLUDED.import_time,
                            store_id = EXCLUDED.store_id;
                        """
                        
                        cursor.execute(insert_query, (
                            data['id'],
                            data['item_id'],
                            data['amount'],
                            import_time,
                            STORE_ID
                        ))
                        conn.commit()
                        logger.info(f"Successfully processed amount for ID: {data['id']}")

                except Exception as e:
                    logger.error(f"Error processing amount message: {str(e)}")
                    continue

    except Exception as e:
        logger.error(f"Fatal error in amount consumer: {str(e)}")
        raise e

    finally:
        logger.info("Closing connections for amount consumer...")
        cursor.close()
        conn.close()
        consumer.close()

# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sync_postgres_to_postgres_transaction_and_amount',
    default_args=default_args,
    description='Sync Customer Transaction and Amount Data in PostgreSQL',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
)

# Define tasks
process_transaction_task = PythonOperator(
    task_id='process_kafka_transaction_messages',
    python_callable=process_kafka_transaction_messages,
    dag=dag,
    execution_timeout=None
)

process_amount_task = PythonOperator(
    task_id='process_kafka_amount_messages',
    python_callable=process_kafka_amount_messages,
    dag=dag,
    execution_timeout=None
)

process_transaction_task
process_amount_task