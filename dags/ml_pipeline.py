from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
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
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
import uuid

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
        'Random Forest': RandomForestRegressor(),
        'XGBoost': XGBRegressor(),
        'Decision Tree': DecisionTreeRegressor()
    }

    scores = {}
    best_score = -float('inf')
    best_model = None
    best_model_name = None
    best_y_pred = None

    for name, model in models.items():
        model.fit(X_train, Y_train)
        y_pred = model.predict(X_test)
        score = metrics.r2_score(Y_test, y_pred)
        scores[name] = score
        print(f"{name} R-squared Score: {score}")
        
        if score > best_score:
            best_score = score
            best_model = model
            best_model_name = name
            best_y_pred = y_pred

    # Save the best model
    os.makedirs(f'{BASE_LOCATION}/airflow/models', exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_name = f'best_model_{timestamp}.pkl'
    model_path = f'{BASE_LOCATION}/airflow/models/{model_name}'
    pickle.dump(best_model, open(model_path, 'wb'))
    print(f"Saved best model: {best_model_name} with R-squared score: {best_score}")

    # Generate charts
    os.makedirs(f'{BASE_LOCATION}/airflow/plots', exist_ok=True)

    # Chart 1: Model Performance Comparison (Bar Chart)
    plt.figure(figsize=(8, 6))
    plt.bar(scores.keys(), scores.values(), color=['#1f77b4', '#ff7f0e', '#2ca02c'])
    plt.title('Model Performance Comparison', fontsize=14, pad=15)
    plt.xlabel('Model', fontsize=12)
    plt.ylabel('R-squared Score', fontsize=12)
    plt.ylim(0, 1)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig(f'{BASE_LOCATION}/airflow/plots/model_comparison.png', bbox_inches='tight')
    plt.close()

    # Chart 2: Feature Importance (for Random Forest or XGBoost)
    if best_model_name in ['Random Forest', 'XGBoost']:
        plt.figure(figsize=(10, 6))
        feature_importance = best_model.feature_importances_
        feature_names = X.columns
        sorted_idx = np.argsort(feature_importance)[-10:]  # Top 10 features
        plt.barh(feature_names[sorted_idx], feature_importance[sorted_idx], color='#9467bd')
        plt.title(f'Feature Importance - {best_model_name}', fontsize=14, pad=15)
        plt.xlabel('Importance', fontsize=12)
        plt.tight_layout()
        plt.savefig(f'{BASE_LOCATION}/airflow/plots/feature_importance.png', bbox_inches='tight')
        plt.close()
    else:
        # Placeholder for Decision Tree
        with open(f'{BASE_LOCATION}/airflow/plots/feature_importance.png', 'wb') as f:
            plt.figure(figsize=(8, 6))
            plt.text(0.5, 0.5, 'Feature Importance Not Available for Decision Tree', 
                     horizontalalignment='center', verticalalignment='center', fontsize=12)
            plt.axis('off')
            plt.savefig(f, format='png', bbox_inches='tight')
            plt.close()

    # Chart 3: Predicted vs Actual Values (Scatter Plot)
    plt.figure(figsize=(8, 6))
    plt.scatter(Y_test, best_y_pred, alpha=0.5, color='#d62728')
    plt.plot([Y_test.min(), Y_test.max()], [Y_test.min(), Y_test.max()], 'k--', lw=2)
    plt.title(f'Predicted vs Actual Values - {best_model_name}', fontsize=14, pad=15)
    plt.xlabel('Actual Item_Store_Sales', fontsize=12)
    plt.ylabel('Predicted Item_Store_Sales', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.savefig(f'{BASE_LOCATION}/airflow/plots/predicted_vs_actual.png', bbox_inches='tight')
    plt.close()

    # Chart 4: Residual Plot
    residuals = Y_test - best_y_pred
    plt.figure(figsize=(8, 6))
    plt.scatter(best_y_pred, residuals, alpha=0.5, color='#17becf')
    plt.axhline(y=0, color='k', linestyle='--', lw=2)
    plt.title(f'Residual Plot - {best_model_name}', fontsize=14, pad=15)
    plt.xlabel('Predicted Item_Store_Sales', fontsize=12)
    plt.ylabel('Residuals', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.savefig(f'{BASE_LOCATION}/airflow/plots/residual_plot.png', bbox_inches='tight')
    plt.close()

    # Generate PDF report
    pdf_path = f'{BASE_LOCATION}/airflow/plots/report_{timestamp}.pdf'
    doc = SimpleDocTemplate(pdf_path, pagesize=letter)
    styles = getSampleStyleSheet()
    story = []

    # Title
    story.append(Paragraph("Machine Learning Pipeline Training Report", styles['Title']))
    story.append(Spacer(1, 0.2 * inch))
    story.append(Paragraph(f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
    story.append(Spacer(1, 0.2 * inch))

    # Summary
    story.append(Paragraph("Summary", styles['Heading2']))
    story.append(Paragraph(f"<b>Best Model:</b> {best_model_name}", styles['Normal']))
    story.append(Paragraph(f"<b>R-squared Score:</b> {best_score:.4f}", styles['Normal']))
    story.append(Paragraph(f"<b>Model Saved At:</b> {model_path}", styles['Normal']))
    story.append(Spacer(1, 0.3 * inch))

    # Charts
    chart_titles = [
        "Model Performance Comparison",
        "Feature Importance",
        "Predicted vs Actual Values",
        "Residual Plot"
    ]
    chart_paths = [
        f'{BASE_LOCATION}/airflow/plots/model_comparison.png',
        f'{BASE_LOCATION}/airflow/plots/feature_importance.png',
        f'{BASE_LOCATION}/airflow/plots/predicted_vs_actual.png',
        f'{BASE_LOCATION}/airflow/plots/residual_plot.png'
    ]

    for title, path in zip(chart_titles, chart_paths):
        story.append(Paragraph(title, styles['Heading2']))
        img = Image(path, width=6 * inch, height=4 * inch)
        img.hAlign = 'CENTER'
        story.append(img)
        story.append(Spacer(1, 0.3 * inch))

    doc.build(story)
    print(f"Generated PDF report at: {pdf_path}")

    # Push results to XCom
    context['ti'].xcom_push(key='model_name', value=model_name)
    context['ti'].xcom_push(key='best_model_name', value=best_model_name)
    context['ti'].xcom_push(key='best_score', value=best_score)
    context['ti'].xcom_push(key='pdf_path', value=pdf_path)

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
    tags=['ml', 'pipeline'],
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
    random_suffix = uuid.uuid4().hex[:6]  # tạo chuỗi ngẫu nhiên 6 ký tự
    container_name = f"backend_{random_suffix}"
    deploy_model = DockerOperator(
        task_id='deploy_model_hosting_server',
        image='backend:latest',
        container_name=container_name,
        api_version='auto',
        auto_remove=True,
        port_bindings={'8000': '8000'},
        environment={
            'MODEL_PATH': '{{ task_instance.xcom_pull(task_ids="train_models", key="model_name") }}'
        },
        command='python3 app.py',
    )
    bestModel = '{{ task_instance.xcom_pull(task_ids="train_models", key="model_name") }}'
    rSquaredScore = '{{ task_instance.xcom_pull(task_ids="train_models", key="best_score") }}'
    savedModel = '{{ task_instance.xcom_pull(task_ids="train_models", key="model_name") }}'

    # Task 5: Send Email Notification
    send_notification = EmailOperator(
        task_id='send_notification',
        to='dungngo0935431740@gmail.com',
        subject='Machine Learning Pipeline Training Report',
        html_content=f"""
        <h2>Machine Learning Pipeline Training Report</h2>
        <p>Dear Team,</p>
        <p>The machine learning pipeline has completed successfully. Below is the summary of the training results:</p>
        <ul>
            <li><strong>Best Model:</strong> {bestModel}</li>
            <li><strong>R-squared Score:</strong> {rSquaredScore}</li>
            <li><strong>Model Saved At:</strong> {BASE_LOCATION}/airflow/models/{savedModel}</li>
        </ul>
        <p>Please find the detailed training report attached as a PDF file, along with individual charts for your reference.</p>
        <p>Best regards,<br>Data Science Team</p>
        """,
        files=[
            '{{ task_instance.xcom_pull(task_ids="train_models", key="pdf_path") }}',
            f'{BASE_LOCATION}/airflow/plots/model_comparison.png',
            f'{BASE_LOCATION}/airflow/plots/feature_importance.png',
            f'{BASE_LOCATION}/airflow/plots/predicted_vs_actual.png',
            f'{BASE_LOCATION}/airflow/plots/residual_plot.png'
        ],
        conn_id='smtp_default',
        mime_charset='utf-8',
    )

    # Define the task sequence
    prepare_data_task >> preprocess_data_task >> train_models_task >> send_notification >> deploy_model