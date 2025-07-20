"""
Bank Data Pipeline DAG
A comprehensive Airflow DAG for banking data processing pipeline that:
1. Generates synthetic banking data
2. Performs data quality checks
3. Uploads valid data to PostgreSQL
4. Logs errors and failures for monitoring
"""

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
        'data_generator': '/opt/airflow/src/generate_data_airflow.py',
        'quality_checker': '/opt/airflow/src/data_quality_standards.py',
        'data_uploader': '/opt/airflow/src/data_uploader.py'
    }

# DAG default arguments
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# def log_task_start(**context):
#     """Log task start"""
#     task_id = context['task_instance'].task_id
#     dag_id = context['dag'].dag_id
#     run_id = context['run_id']
    
#     logger = PipelineLogger()
#     logger.log_task_start(
#         run_id=run_id,
#         dag_id=dag_id,
#         task_id=task_id,
#         execution_date=context['execution_date']
#     )
#     print(f"✅ Task {task_id} started for run {run_id}")


from datetime import datetime

def log_task_start(**context):
    """Log task start"""
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    
    execution_date = context['execution_date']

    # Chuyển đổi execution_date về datetime thực nếu cần
    if not isinstance(execution_date, datetime):
        execution_date = datetime.fromisoformat(str(execution_date))

    logger = PipelineLogger()
    logger.log_task_start(
        run_id=run_id,
        dag_id=dag_id,
        task_id=task_id,
        execution_date=execution_date
    )

    print(f"✅ Task {task_id} started for run {run_id}")



def generate_banking_data(**context):
    """Generate synthetic banking data"""
    import subprocess
    
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = PipelineLogger()
    
    try:
        logger.log_info(f"Starting data generation for run {run_id}")
        logger.log_info(f"Script path: {SCRIPT_PATHS['data_generator']}")
        # Set environment variables
        env = os.environ.copy()
        env['CUSTOMER_COUNT'] = str(DATA_GEN_CONFIG.customer_count if 'DATA_GEN_CONFIG' in globals() else '10')
        
        # Run data generation script
        result = subprocess.run(
            ['python', SCRIPT_PATHS['data_generator']], 
            capture_output=True, 
            text=True,
            env=env,
            timeout=1800  # 30 minutes timeout
        )
        
        if result.returncode != 0:
            error_msg = f"Data generation failed: {result.stderr}"
            logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
            raise AirflowException(error_msg)
        
        logger.log_info(f"Data generation completed successfully: {result.stdout}")
        logger.log_task_success(run_id, 'bank_data_pipeline', task_id)
        
        return {"status": "success", "message": "Data generated successfully"}
        
    except subprocess.TimeoutExpired:
        error_msg = "Data generation timed out after 30 minutes"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error in data generation: {str(e)}"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg, str(e))
        raise

def run_data_quality_checks(**context):
    """Run comprehensive data quality checks"""
    import subprocess
    
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = PipelineLogger()
    
    try:
        logger.log_info(f"Starting data quality checks for run {run_id}")
        
        # Run quality check script
        result = subprocess.run(
            ['python', SCRIPT_PATHS['quality_checker']], 
            capture_output=True, 
            text=True,
            timeout=600  # 10 minutes timeout
        )
        
        if result.returncode != 0:
            error_msg = f"Data quality check failed: {result.stderr}"
            logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
            # Push failure info to XCom for downstream tasks
            context['task_instance'].xcom_push(key='quality_check_failed', value=True)
            context['task_instance'].xcom_push(key='quality_error_details', value=result.stderr)
            raise AirflowException(error_msg)
        
        logger.log_info(f"Data quality checks passed: {result.stdout}")
        logger.log_task_success(run_id, 'bank_data_pipeline', task_id)
        context['task_instance'].xcom_push(key='quality_check_failed', value=False)
        
        return {"status": "success", "message": "Data quality checks passed"}
        
    except subprocess.TimeoutExpired:
        error_msg = "Data quality check timed out after 10 minutes"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error in quality checks: {str(e)}"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg, str(e))
        raise

def upload_data_to_postgres(**context):
    """Upload validated data to PostgreSQL"""
    import subprocess
    
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = PipelineLogger()
    
    try:
        logger.log_info(f"Starting data upload for run {run_id}")
        
        # Run data upload script
        result = subprocess.run(
            ['python', SCRIPT_PATHS['data_uploader']], 
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

def log_pipeline_failure(**context):
    """Log pipeline failure details"""
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = PipelineLogger()
    
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
        
        print(f"❌ Pipeline failure logged for run {run_id}")
        
    except Exception as e:
        print(f"Error logging pipeline failure: {str(e)}")

def cleanup_and_notify(**context):
    """Cleanup resources and send notifications"""
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    logger = PipelineLogger()
    
    try:
        # Log successful completion
        logger.log_info(f"Pipeline completed successfully for run {run_id}")
        logger.log_task_success(run_id, 'bank_data_pipeline', task_id)
        
        # You can add notification logic here (email, Slack, etc.)
        print(f"✅ Pipeline completed successfully for run {run_id}")
        
        return {"status": "success", "message": "Pipeline completed successfully"}
        
    except Exception as e:
        error_msg = f"Error in cleanup: {str(e)}"
        logger.log_error(run_id, 'bank_data_pipeline', task_id, error_msg)
        print(f"⚠️ Cleanup error: {error_msg}")

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

with open('/opt/airflow/sql/create_log_tables.sql', 'r') as f:
    create_log_sql = f.read()

init_logging_tables = PostgresOperator(
    task_id='init_logging_tables',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=create_log_sql,  # raw SQL content
    dag=dag
)

# Task 2: Log pipeline start
log_start = PythonOperator(
    task_id='log_pipeline_start',
    python_callable=log_task_start,
    dag=dag,
    doc_md="Log the start of pipeline execution"
)

# Task 3: Generate banking data
generate_data = PythonOperator(
    task_id='generate_banking_data',
    python_callable=generate_banking_data,
    dag=dag,
    doc_md="Generate synthetic banking data including customers, accounts, transactions"
)

# Task 4: Run data quality checks
quality_checks = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
    doc_md="Run comprehensive data quality checks on generated data"
)

# Task 5: Upload data to PostgreSQL
upload_data = PythonOperator(
    task_id='upload_data_to_postgres',
    python_callable=upload_data_to_postgres,
    trigger_rule='none_failed',  # Only run if quality checks pass
    dag=dag,
    doc_md="Upload validated data to PostgreSQL database"
)

# Task 6: Log failures (runs if any task fails)
log_failures = PythonOperator(
    task_id='log_pipeline_failures',
    python_callable=log_pipeline_failure,
    trigger_rule='one_failed',  # Run if any upstream task fails
    dag=dag,
    doc_md="Log pipeline failures and error details"
)

# Task 7: Cleanup and notify success
cleanup_notify = PythonOperator(
    task_id='cleanup_and_notify',
    python_callable=cleanup_and_notify,
    trigger_rule='none_failed',  # Only run if all upstream tasks succeed
    dag=dag,
    doc_md="Cleanup resources and send success notifications"
)

# Define task dependencies
init_logging_tables >> log_start >> generate_data >> quality_checks

# Branching: If quality checks pass -> upload data, if fail -> log failures
quality_checks >> [upload_data, log_failures]

# Success path: upload -> cleanup
upload_data >> cleanup_notify

# The log_failures task is terminal for the failure path

# Add task documentation
dag.doc_md = """
# Banking Data Pipeline

This DAG implements a comprehensive data pipeline for banking data processing:

## Pipeline Steps:
1. **Initialize Logging**: Create necessary logging tables
2. **Log Start**: Record pipeline execution start
3. **Generate Data**: Create synthetic banking data (customers, accounts, transactions)
4. **Quality Checks**: Validate data quality and business rules
5. **Upload Data**: Store validated data in PostgreSQL (only if quality checks pass)
6. **Log Failures**: Record any failures for monitoring (runs on failure)
7. **Cleanup**: Cleanup and notify success (runs on success)

## Key Features:
- ✅ Comprehensive error handling and logging
- ✅ Data quality validation with business rules
- ✅ Conditional execution based on quality check results
- ✅ Detailed monitoring and error tracking
- ✅ Timeout protection for long-running tasks

## Configuration:
- Schedule: Daily execution
- Max Active Runs: 1 (prevents overlapping runs)
- Retries: 2 attempts with 5-minute delay
- Email notifications on failure

## Monitoring:
Check the following tables for pipeline monitoring:
- `pipeline_execution_log`: Task execution details
- `data_quality_log`: Quality check results
- `pipeline_error_log`: Error details and stack traces
"""