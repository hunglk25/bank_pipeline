import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, Any
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago
from dotenv import load_dotenv

# Add project root to Python path
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/src')
sys.path.append('/opt/airflow/config')

load_dotenv(dotenv_path="/opt/airflow/.env") 

SCRIPT_PATHS = {
    'data_generator': os.getenv('DATA_GENERATOR_SCRIPT'),
    'quality_checker': os.getenv('QUALITY_CHECKER_SCRIPT'),
    'data_uploader': os.getenv('DATA_UPLOADER_SCRIPT'),
    'monitoring_audit': os.getenv('MONITORING_AUDIT_SCRIPT'),
}

def setup_task_logger(run_id, task_id):
    """Setup logger for specific task and run"""
    log_dir = f"/opt/airflow/logs/run_{run_id.replace(':', '_')}"
    os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger(f"{run_id}_{task_id}")
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # File handler
    handler = logging.FileHandler(f"{log_dir}/{task_id}.log")
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

# DAG default arguments
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'catchup': False,
}

def generate_banking_data(**context):
    """Generate synthetic banking data and return output directory"""
    import subprocess
    import os
    from datetime import datetime

    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = setup_task_logger(run_id, task_id)

    try:
        logger.info(f"Starting data generation for run {run_id}")

        base_dir = "/tmp/generated_data"
        os.makedirs(base_dir, exist_ok=True)
        output_dir = os.path.join(base_dir, f"run_{run_id.replace(':', '_')}")
        os.makedirs(output_dir, exist_ok=True)
        # Set environment variables
        env = os.environ.copy()

        result = subprocess.run(
            ['python', SCRIPT_PATHS['data_generator'], '--output_dir', output_dir],
            capture_output=True,
            text=True,
            env=env,
            timeout=1800
        )

        logger.info(f"Data generation output: {result.stdout}")

        if result.returncode != 0:
            error_msg = f"Data generation failed: {result.stderr}"
            logger.error(error_msg)
            raise AirflowException(error_msg)

        logger.info("Task completed successfully")

        ti = context['task_instance']
        ti.xcom_push(key='data_output_path', value=output_dir)

        return {"status": "success", "path": output_dir}

    except subprocess.TimeoutExpired:
        error_msg = "Data generation timed out after 30 minutes"
        logger.error(error_msg)
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error in data generation: {str(e)}"
        logger.error(f"{error_msg} - {str(e)}")
        raise


def run_data_quality_checks(**context):
    """Run comprehensive data quality checks using generated output"""
    import subprocess

    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = setup_task_logger(run_id, task_id)

    try:
        logger.info(f"Starting data quality checks for run {run_id}")

        # Lấy path từ XCom
        ti = context['ti']
        data_path = ti.xcom_pull(task_ids='generate_banking_data', key='data_output_path')
        if not data_path:
            raise AirflowException("No data path found from previous task.")

        # Truyền path vào script kiểm tra
        result = subprocess.run(
            ['python', SCRIPT_PATHS['quality_checker'], '--input_dir', data_path],
            capture_output=True,
            text=True,
            timeout=600
        )

        if result.returncode != 0:
            error_msg = f"Data quality check failed: {result.stderr}"
            logger.error(error_msg)
            ti.xcom_push(key='quality_check_failed', value=True)
            ti.xcom_push(key='quality_error_details', value=result.stderr)
            raise AirflowException(error_msg)

        logger.info(f"Data quality checks passed: {result.stdout}")
        logger.info("Task completed successfully")
        ti.xcom_push(key='quality_check_failed', value=False)

        return {"status": "success", "message": "Data quality checks passed"}

    except subprocess.TimeoutExpired:
        error_msg = "Data quality check timed out after 10 minutes"
        logger.error(error_msg)
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error in quality checks: {str(e)}"
        logger.error(f"{error_msg} - {str(e)}")
        raise

def run_risk_alerts(**context):
    """Run risk alert checks using generated output"""
    import subprocess

    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = setup_task_logger(run_id, task_id)

    try:
        logger.info(f"Starting risk alert checks for run {run_id}")

        # Lấy path từ XCom
        ti = context['ti']
        data_path = ti.xcom_pull(task_ids='generate_banking_data', key='data_output_path')
        if not data_path:
            raise AirflowException("No data path found from previous task.")

        # Truyền path vào script kiểm tra
        result = subprocess.run(
            ['python', SCRIPT_PATHS['monitoring_audit'], '--dir', data_path],
            capture_output=True,
            text=True,
            timeout=600
        )

        if result.returncode != 0:
            error_msg = f"Risk alert check failed: {result.stderr}"
            logger.error(error_msg)
            raise AirflowException(error_msg)

        logger.info(f"Risk alert checks passed: {result.stdout}")
        logger.info("Task completed successfully")

        return {"status": "success", "message": "Risk alert checks passed"}

    except subprocess.TimeoutExpired:
        error_msg = "Risk alert check timed out after 10 minutes"
        logger.error(error_msg)
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error in risk alerts: {str(e)}"
        logger.error(f"{error_msg} - {str(e)}")
        raise


def upload_data_to_postgres(**context):
    """Upload validated data to PostgreSQL"""
    import subprocess
    
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = setup_task_logger(run_id, task_id)
    
    try:
        logger.info(f"Starting data upload for run {run_id}")

        ti = context['ti']
        dir = ti.xcom_pull(task_ids='generate_banking_data', key='data_output_path')
        
        # Run data upload script
        result = subprocess.run(
            ['python', SCRIPT_PATHS['data_uploader'], '--dir', dir], 
            capture_output=True, 
            text=True,
            timeout=1200  # 20 minutes timeout
        )
        
        if result.returncode != 0:
            error_msg = f"Data upload failed: {result.stderr}"
            logger.error(error_msg)
            raise AirflowException(error_msg)
        
        logger.info(f"Data upload completed successfully: {result.stdout}")
        logger.info("Task completed successfully")
        
        return {"status": "success", "message": "Data uploaded successfully"}
        
    except subprocess.TimeoutExpired:
        error_msg = "Data upload timed out after 20 minutes"
        logger.error(error_msg)
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error in data upload: {str(e)}"
        logger.error(f"{error_msg} - {str(e)}")
        raise

def log_pipeline_failures(**context):
    """Log pipeline failure details"""
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = setup_task_logger(run_id, task_id)
    
    try:
        # Get failure information from upstream tasks
        quality_failed = context['task_instance'].xcom_pull(
            task_ids='run_data_quality_checks', 
            key='quality_check_failed'
        )
        
        if quality_failed:
            error_details = context['task_instance'].xcom_pull(
                task_ids='run_data_quality_checks', 
                key='quality_error_details'
            )
            logger.error(f"Quality check failures logged: {error_details}")
        
        # Log general pipeline failure
        logger.error("Pipeline failed - check upstream task logs for details")
        
        print(f"Pipeline failure logged for run {run_id}")
        
    except Exception as e:
        logger.error(f"Error logging pipeline failure: {str(e)}")
        print(f"Error logging pipeline failure: {str(e)}")

def notify(**context):
    """Send notifications"""
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = setup_task_logger(run_id, task_id)
    
    try:
        # Log successful completion
        logger.info(f"Pipeline completed successfully for run {run_id}")
        logger.info("Task completed successfully")
        
        # You can add notification logic here (email, Slack, etc.)
        print(f"Pipeline completed successfully for run {run_id}")
        
        return {"status": "success", "message": "Pipeline completed successfully"}
        
    except Exception as e:
        error_msg = f"Error in cleanup: {str(e)}"
        logger.error(error_msg)
        print(f"Cleanup error: {error_msg}")

# Create the DAG
dag = DAG(
    'bank_data_pipeline',
    default_args=default_args,
    description='Banking Data Pipeline - Generate, Validate, and Upload Data',
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False,
    tags=['banking', 'data-pipeline', 'etl'],
    doc_md=__doc__
)

# Task 1: Generate banking data
generate_data = PythonOperator(
    task_id='generate_banking_data',
    python_callable=generate_banking_data,
    dag=dag,
    doc_md="Generate synthetic banking data including customers, accounts, transactions"
)

# Task 2: Run data quality checks
quality_checks = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
    doc_md="Run comprehensive data quality checks on generated data"
)

# Task 3: Run risk alert checks
risk_alerts = PythonOperator(
    task_id='risk_alerts',
    python_callable=run_risk_alerts,
    dag=dag,
    doc_md="Run risk alert checks on generated data"
)
    

# Task 4: Upload data to PostgreSQL
upload_data = PythonOperator(
    task_id='upload_data_to_postgres',
    python_callable=upload_data_to_postgres,
    trigger_rule='none_failed',  # Only run if quality checks pass
    dag=dag,
    doc_md="Upload validated data to PostgreSQL database"
)

# Task 5: Log failures (runs if any task fails)
log_failures = PythonOperator(
    task_id='log_pipeline_failures',
    python_callable=log_pipeline_failures,
    trigger_rule='one_failed',  # Run if any upstream task fails
    dag=dag,
    doc_md="Log pipeline failures and error details"
)

# Task 6: Cleanup and notify success
cleanup_notify = PythonOperator(
    task_id='cleanup_and_notify',
    python_callable=notify,
    trigger_rule='none_failed',  # Only run if all upstream tasks succeed
    dag=dag,
    doc_md="Cleanup resources and send success notifications"
)

# Define task dependencies
generate_data >> quality_checks >> risk_alerts

# Branching: If quality checks pass -> upload data, if fail -> log failures
risk_alerts >> [upload_data, log_failures]

# Success path: upload -> cleanup
upload_data >> cleanup_notify

# The log_failures task is terminal for the failure