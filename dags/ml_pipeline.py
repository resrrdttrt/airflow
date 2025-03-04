from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import numpy as np
import pandas as pd
import os
from datetime import datetime
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn import metrics
import pickle
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.tree import DecisionTreeRegressor

BASE_LOCATION = '/opt'

def prepare_data(**context):
    # Data Loading
    data_Item_Store_Sale = pd.read_csv(f'{BASE_LOCATION}/airflow/dataset/Item_Store_Sales.csv')
    data_Stores = pd.read_csv(f'{BASE_LOCATION}/airflow/dataset/Stores.csv')
    data_Items = pd.read_csv(f'{BASE_LOCATION}/airflow/dataset/Items.csv')

    # Data Merging
    data_frame = pd.merge(data_Item_Store_Sale, data_Items, on='Item_Id')
    data_frame = pd.merge(data_frame, data_Stores, on='Store_Id')
    
    # Save temporary result
    os.makedirs(f'{BASE_LOCATION}/airflow/temporary', exist_ok=True)
    data_frame.to_csv(f'{BASE_LOCATION}/airflow/temporary/merged_data.csv', index=False)
    print("Data preparation completed")

def preprocess_data(**context):
    # Load merged data
    data_frame = pd.read_csv(f'{BASE_LOCATION}/airflow/temporary/merged_data.csv')
    
    # Handle missing values
    data_frame['Item_Fabric_Amount'] = data_frame['Item_Fabric_Amount'].fillna(data_frame['Item_Fabric_Amount'].mean())
    mode_of_store_size = data_frame.pivot_table(values='Store_Size', columns='Store_Type', aggfunc=lambda x: x.mode()[0])
    miss_values = data_frame['Store_Size'].isnull()
    data_frame.loc[miss_values, 'Store_Size'] = data_frame.loc[miss_values, 'Store_Type'].apply(lambda x: mode_of_store_size[x])

    # Encode categorical variables
    encoder = LabelEncoder()
    categorical_columns = ['Store_Id', 'Store_Size', 'Store_Location_Type', 'Store_Type',
                         'Item_Id', 'Item_Fit_Type', 'Item_Fabric']
    for col in categorical_columns:
        data_frame[col] = encoder.fit_transform(data_frame[col])

    # Save preprocessed data
    data_frame.to_csv(f'{BASE_LOCATION}/airflow/temporary/preprocessed_data.csv', index=False)
    print("Data preprocessing completed")

def train_models(**context):
    # Load preprocessed data
    data_frame = pd.read_csv(f'{BASE_LOCATION}/airflow/temporary/preprocessed_data.csv')
    
    # Split data
    X = data_frame.drop(columns='Item_Store_Sales', axis=1)
    Y = data_frame['Item_Store_Sales']
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=2)

    # Train and evaluate models
    models = {
        'random_forest': RandomForestRegressor(),
        'xgboost': XGBRegressor(),
        'decision_tree': DecisionTreeRegressor()
    }

    best_score = -float('inf')
    best_model = None
    best_model_name = None

    for name, model in models.items():
        model.fit(X_train, Y_train)
        y_pred = model.predict(X_test)
        score = metrics.r2_score(Y_test, y_pred)
        print(f"{name} R-squared Score: {score}")
        
        if score > best_score:
            best_score = score
            best_model = model
            best_model_name = name

    # Save the best model
    os.makedirs(f'{BASE_LOCATION}/airflow/models', exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_name = f'best_model_{timestamp}.pkl'
    model_path = f'{BASE_LOCATION}/airflow/models/{model_name}'
    pickle.dump(best_model, open(model_path, 'wb'))
    print(f"Saved best model: {best_model_name} with R-squared score: {best_score}")
    context['ti'].xcom_push(key='model_name', value=model_name)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Create the DAG
with DAG(
    dag_id='ml_pipeline',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
) as dag:

    # Task 1: Data Preparation
    prepare_data_task = PythonOperator(
        task_id='prepare_data',
        python_callable=prepare_data,
    )

    # Task 2: Data Preprocessing
    preprocess_data_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
    )

    # Task 3: Model Training
    train_models_task = PythonOperator(
        task_id='train_models',
        python_callable=train_models,
    )

    # Task 4: Deploy Model
    deploy_model = DockerOperator(
        task_id='deploy_model_hosting_server',
        image='backend',
        container_name='backend',
        api_version='auto',
        auto_remove=True,
        mounts=[
            {
                'source': '/home/vandung545917/airflow/models',
                'target': '/app/models',
                'type': 'bind',
            },
        ],
        port_bindings={'8000': '8000'},
        environment={
            'MODEL_NAME': '{{ task_instance.xcom_pull(task_ids="train_models", key="model_name") }}'
        },
        command='python3 app.py',
    )

    # Define the task sequence
    prepare_data_task >> preprocess_data_task >> train_models_task >> deploy_model