import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago

# # Add project root to Python path
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/src')
sys.path.append('/opt/airflow/config')

# Import project modules
try:
    from config.pipeline_config import (
        PIPELINE_CONFIG, 
        DB_CONFIG, 
        DATA_GEN_CONFIG,
        POSTGRES_CONN_ID,
        SCRIPT_PATHS
    )
    from src.pipeline_logger import PipelineLogger
except ImportError as e:
    print(f"Warning: Could not import config modules: {e}")
    # Fallback configuration
    POSTGRES_CONN_ID = 'postgres_data'
    SCRIPT_PATHS = {
        'data_generator': '/opt/airflow/src/generate_data.py',
        'quality_checker': '/opt/airflow/src/data_quality.py',
        'data_uploader': '/opt/airflow/src/data_uploader.py',
        'monitoring_audit': '/opt/airflow/src/monitoring_audit.py'
    }

SCRIPT_PATHS['data_generator'] = '/opt/airflow/src/generate_data.py'
SCRIPT_PATHS['quality_checker'] = '/opt/airflow/src/data_quality.py'
SCRIPT_PATHS['data_uploader'] = '/opt/airflow/src/data_uploader.py'
SCRIPT_PATHS['monitoring_audit'] = '/opt/airflow/src/monitoring_audit.py'

# DAG default arguments
default_args = {
    'owner': 'data_engineering_team_2',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

def log_task_start(**context):
    """Log task start"""
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    
    # Convert execution_date to datetime object to avoid Proxy issues
    execution_date = context['execution_date']
    if hasattr(execution_date, 'datetime'):
        execution_date = execution_date.datetime
    elif hasattr(execution_date, 'to_datetime'):
        execution_date = execution_date.to_datetime()
    
    logger = PipelineLogger()
    logger.log_task_start(
        run_id=run_id,
        dag_id=dag_id,
        task_id=task_id,
        execution_date=execution_date
    )
    print(f"✅ Task {task_id} started for run {run_id}")


def generate_banking_data(**context):
    """Generate synthetic banking data and return output directory"""
    import subprocess
    import os
    from datetime import datetime

    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = PipelineLogger()

    try:
        logger.log_info(f"Starting data generation for run {run_id}")

        # Tạo output dir riêng cho từng run
        base_dir = "/tmp/generated_data"
        os.makedirs(base_dir, exist_ok=True)
        output_dir = os.path.join(base_dir, f"run_{run_id.replace(':', '_')}")
        os.makedirs(output_dir, exist_ok=True)
        # Set environment variables
        env = os.environ.copy()
        env['CUSTOMER_COUNT'] = str(DATA_GEN_CONFIG.customer_count if 'DATA_GEN_CONFIG' in globals() else '10')

        # Gọi script, truyền output_dir
        result = subprocess.run(
            ['python', SCRIPT_PATHS['data_generator'], '--output_dir', output_dir],
            capture_output=True,
            text=True,
            env=env,
            timeout=1800
        )

        logger.log_info(f"Data generation output: {result.stdout}")

        if result.returncode != 0:
            error_msg = f"Data generation failed: {result.stderr}"
            logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
            raise AirflowException(error_msg)

        # Lấy path từ stdout (nên chỉ in 1 dòng path trong script)
        logger.log_task_success(run_id, 'bank_data_pipeline', task_id)

        # Push path để task sau dùng
        ti = context['task_instance']
        ti.xcom_push(key='data_output_path', value=output_dir)

        return {"status": "success", "path": output_dir}

    except subprocess.TimeoutExpired:
        error_msg = "Data generation timed out after 30 minutes"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error in data generation: {str(e)}"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg, str(e))
        raise


def run_data_quality_checks_2(**context):
    """Run comprehensive data quality checks using generated output"""
    import subprocess

    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = PipelineLogger()

    try:
        logger.log_info(f"Starting data quality checks for run {run_id}")

        # Lấy path từ XCom
        ti = context['ti']
        data_path = ti.xcom_pull(task_ids='generate_banking_data_2', key='data_output_path')
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
            logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
            ti.xcom_push(key='quality_check_failed', value=True)
            ti.xcom_push(key='quality_error_details', value=result.stderr)
            raise AirflowException(error_msg)

        logger.log_info(f"Data quality checks passed: {result.stdout}")
        logger.log_task_success(run_id, 'bank_data_pipeline', task_id)
        ti.xcom_push(key='quality_check_failed', value=False)

        return {"status": "success", "message": "Data quality checks passed"}

    except subprocess.TimeoutExpired:
        error_msg = "Data quality check timed out after 10 minutes"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error in quality checks: {str(e)}"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg, str(e))
        raise

def run_risk_alerts_2(**context):
    """Run risk alert checks using generated output"""
    import subprocess

    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = PipelineLogger()

    try:
        logger.log_info(f"Starting risk alert checks for run {run_id}")

        # Lấy path từ XCom
        ti = context['ti']
        data_path = ti.xcom_pull(task_ids='generate_banking_data_2', key='data_output_path')
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
            logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
            raise AirflowException(error_msg)

        logger.log_info(f"Risk alert checks passed: {result.stdout}")
        logger.log_task_success(run_id, 'bank_data_pipeline', task_id)

        return {"status": "success", "message": "Risk alert checks passed"}

    except subprocess.TimeoutExpired:
        error_msg = "Risk alert check timed out after 10 minutes"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error in risk alerts: {str(e)}"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg, str(e))
        raise


def upload_data_to_postgres_2(**context):
    """Upload validated data to PostgreSQL"""
    import subprocess
    
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = PipelineLogger()
    
    try:
        logger.log_info(f"Starting data upload for run {run_id}")

        ti = context['ti']
        dir = ti.xcom_pull(task_ids='generate_banking_data_2', key='data_output_path')
        
        # Run data upload script
        result = subprocess.run(
            ['python', SCRIPT_PATHS['data_uploader'], '--dir', dir], 
            capture_output=True, 
            text=True,
            timeout=1200  # 20 minutes timeout
        )
        
        if result.returncode != 0:
            error_msg = f"Data upload failed: {result.stderr}"
            logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
            raise AirflowException(error_msg)
        
        logger.log_info(f"Data upload completed successfully: {result.stdout}")
        logger.log_task_success(run_id, 'bank_data_pipeline', task_id)
        
        return {"status": "success", "message": "Data uploaded successfully"}
        
    except subprocess.TimeoutExpired:
        error_msg = "Data upload timed out after 20 minutes"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error in data upload: {str(e)}"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg, str(e))
        raise

def log_pipeline_failure_2(**context):
    """Log pipeline failure details"""
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = PipelineLogger()
    
    try:
        # Get failure information from upstream tasks
        quality_failed = context['task_instance'].xcom_pull(
            task_ids='run_data_quality_checks_2', 
            key='quality_check_failed'
        )
        
        if quality_failed:
            error_details = context['task_instance'].xcom_pull(
                task_ids='run_data_quality_checks_2', 
                key='quality_error_details'
            )
            logger.log_error(
                run_id, 
                'bank_data_pipeline', 
                'quality_failure_handler',
                f"Quality check failures logged: {error_details}"
            )
        
        # Log general pipeline failure
        logger.log_error(
            run_id, 
            'bank_data_pipeline', 
            task_id,
            "Pipeline failed - check upstream task logs for details"
        )
        
        print(f"Pipeline failure logged for run {run_id}")
        
    except Exception as e:
        print(f"Error logging pipeline failure: {str(e)}")

def notify_2(**context):
    """Send notifications"""
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = PipelineLogger()
    
    try:
        # Log successful completion
        logger.log_info(f"Pipeline completed successfully for run {run_id}")
        logger.log_task_success(run_id, 'bank_data_pipeline', task_id)
        
        # You can add notification logic here (email, Slack, etc.)
        print(f"Pipeline completed successfully for run {run_id}")
        
        return {"status": "success", "message": "Pipeline completed successfully"}
        
    except Exception as e:
        error_msg = f"Error in cleanup: {str(e)}"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
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


# Task 2: Log pipeline start
log_start_2 = PythonOperator(
    task_id='log_pipeline_start_2',
    python_callable=log_task_start,
    dag=dag,
    doc_md="Log the start of pipeline execution"
)

# Task 3: Generate banking data
generate_data_2 = PythonOperator(
    task_id='generate_banking_data_2',
    python_callable=generate_banking_data,
    dag=dag,
    doc_md="Generate synthetic banking data including customers, accounts, transactions"
)

# Task 4: Run data quality checks
quality_checks_2 = PythonOperator(
    task_id='run_data_quality_checks_2',
    python_callable=run_data_quality_checks_2,
    dag=dag,
    doc_md="Run comprehensive data quality checks on generated data"
)

risk_alerts_2 = PythonOperator(
    task_id='risk_alerts_2',
    python_callable=run_risk_alerts_2,
    dag=dag,
    doc_md="Run risk alert checks on generated data"
)
    

# Task 5: Upload data to PostgreSQL
upload_data_2 = PythonOperator(
    task_id='upload_data_to_postgres_2',
    python_callable=upload_data_to_postgres_2,
    trigger_rule='none_failed',  # Only run if quality checks pass
    dag=dag,
    doc_md="Upload validated data to PostgreSQL database"
)

# Task 6: Log failures (runs if any task fails)
log_failures_2 = PythonOperator(
    task_id='log_pipeline_failures_2',
    python_callable=log_pipeline_failure_2,
    trigger_rule='one_failed',  # Run if any upstream task fails
    dag=dag,
    doc_md="Log pipeline failures and error details"
)

# Task 7: Cleanup and notify success
cleanup_notify_2 = PythonOperator(
    task_id='cleanup_and_notify_2',
    python_callable=notify_2,
    trigger_rule='none_failed',  # Only run if all upstream tasks succeed
    dag=dag,
    doc_md="Cleanup resources and send success notifications"
)

# Define task dependencies
log_start_2 >> generate_data_2 >> quality_checks_2 >> risk_alerts_2

# Branching: If quality checks pass -> upload data, if fail -> log failures
risk_alerts_2 >> [upload_data_2, log_failures_2]

# Success path: upload -> cleanup
upload_data_2 >> cleanup_notify_2

# The log_failures task is terminal for the failure path
