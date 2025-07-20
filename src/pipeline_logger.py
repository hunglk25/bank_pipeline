"""
Pipeline Logger Module
Centralized logging utility for the banking data pipeline
Handles database logging, error tracking, and monitoring
"""

import logging
import psycopg2
import json
import traceback
from datetime import datetime
from typing import Optional, Dict, Any
import os
import sys

class PipelineLogger:
    """Centralized logger for the data pipeline"""
    
    def __init__(self, 
                 host: str = "postgres_data",
                 port: int = 5432,
                 database: str = "mydata", 
                 user: str = "user",
                 password: str = "userpass"):
        """Initialize the pipeline logger"""
        
        self.conn_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        
        # Setup Python logging
        self.logger = logging.getLogger('pipeline_logger')
        self.logger.setLevel(logging.INFO)
        
        # Create console handler if not exists
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def get_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(**self.conn_params)
            return conn
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def log_task_start(self, run_id: str, dag_id: str, task_id: str, execution_date: datetime):
        """Log task start"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO pipeline_execution_log 
                (run_id, dag_id, task_id, execution_date, start_time, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, task_id) DO UPDATE SET
                    start_time = EXCLUDED.start_time,
                    status = EXCLUDED.status
            """, (run_id, dag_id, task_id, execution_date, datetime.now(), 'RUNNING'))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info(f"Task {task_id} started for run {run_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to log task start: {str(e)}")
    
    def log_task_success(self, run_id: str, dag_id: str, task_id: str, records_processed: int = 0):
        """Log successful task completion"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE pipeline_execution_log 
                SET end_time = %s, status = %s, records_processed = %s
                WHERE run_id = %s AND task_id = %s
            """, (datetime.now(), 'SUCCESS', records_processed, run_id, task_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info(f"Task {task_id} completed successfully for run {run_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to log task success: {str(e)}")
    
    def log_task_failure(self, run_id: str, dag_id: str, task_id: str, error_message: str):
        """Log task failure"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE pipeline_execution_log 
                SET end_time = %s, status = %s, error_message = %s
                WHERE run_id = %s AND task_id = %s
            """, (datetime.now(), 'FAILED', error_message, run_id, task_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.error(f"Task {task_id} failed for run {run_id}: {error_message}")
            
        except Exception as e:
            self.logger.error(f"Failed to log task failure: {str(e)}")
    
    def log_quality_check(self, run_id: str, table_name: str, check_type: str, 
                         status: str, issue_count: int = 0, issues: list = None):
        """Log data quality check results"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            issues_json = json.dumps(issues or [])
            
            cursor.execute("""
                INSERT INTO data_quality_log 
                (run_id, table_name, check_type, issue_count, issues_json, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (run_id, table_name, check_type, issue_count, issues_json, status))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info(f"Quality check {check_type} on {table_name}: {status} ({issue_count} issues)")
            
        except Exception as e:
            self.logger.error(f"Failed to log quality check: {str(e)}")
    
    def log_upload_error(self, run_id: str, table_name: str, error_type: str, 
                        error_message: str, failed_record: dict = None):
        """Log data upload errors"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            failed_record_json = json.dumps(failed_record or {})
            
            cursor.execute("""
                INSERT INTO data_upload_error_log 
                (run_id, table_name, error_type, error_message, failed_record_json)
                VALUES (%s, %s, %s, %s, %s)
            """, (run_id, table_name, error_type, error_message, failed_record_json))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.error(f"Upload error in {table_name}: {error_message}")
            
        except Exception as e:
            self.logger.error(f"Failed to log upload error: {str(e)}")
    
    def log_error(self, run_id: str, dag_id: str, task_id: str, error_message: str, 
                  stack_trace: str = None, context_data: dict = None, severity: str = 'ERROR'):
        """Log general pipeline errors"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get stack trace if not provided
            if stack_trace is None:
                stack_trace = traceback.format_exc()
            
            context_json = json.dumps(context_data or {})
            
            cursor.execute("""
                INSERT INTO pipeline_error_log 
                (run_id, dag_id, task_id, error_message, stack_trace, context_data, severity)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (run_id, dag_id, task_id, error_message, stack_trace, context_json, severity))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.error(f"Pipeline error in {task_id}: {error_message}")
            
        except Exception as e:
            self.logger.error(f"Failed to log pipeline error: {str(e)}")
    
    def log_data_statistics(self, run_id: str, table_name: str, record_count: int,
                           null_count: int = 0, duplicate_count: int = 0,
                           processing_time_ms: int = 0):
        """Log data statistics for monitoring"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO data_statistics_log 
                (run_id, table_name, record_count, null_count, duplicate_count, avg_processing_time_ms)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (run_id, table_name, record_count, null_count, duplicate_count, processing_time_ms))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info(f"Statistics for {table_name}: {record_count} records processed")
            
        except Exception as e:
            self.logger.error(f"Failed to log statistics: {str(e)}")
    
    def log_info(self, message: str):
        """Log info message"""
        self.logger.info(message)
    
    def log_warning(self, message: str):
        """Log warning message"""
        self.logger.warning(message)
    
    def log_debug(self, message: str):
        """Log debug message"""
        self.logger.debug(message)
    
    def get_pipeline_health(self, days: int = 7) -> Dict[str, Any]:
        """Get pipeline health summary for the last N days"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    DATE(execution_date) as date,
                    dag_id,
                    COUNT(*) as total_runs,
                    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_runs,
                    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_runs,
                    AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration_seconds
                FROM pipeline_execution_log 
                WHERE execution_date >= CURRENT_DATE - INTERVAL '%s days'
                GROUP BY DATE(execution_date), dag_id
                ORDER BY date DESC
            """, (days,))
            
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            
            health_data = []
            for row in results:
                health_data.append({
                    'date': row[0].isoformat() if row[0] else None,
                    'dag_id': row[1],
                    'total_runs': row[2],
                    'successful_runs': row[3],
                    'failed_runs': row[4],
                    'avg_duration_seconds': float(row[5]) if row[5] else 0
                })
            
            return {
                'status': 'success',
                'data': health_data,
                'summary': {
                    'total_days': days,
                    'total_health_records': len(health_data)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get pipeline health: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'data': []
            }
    
    def get_recent_errors(self, hours: int = 24) -> Dict[str, Any]:
        """Get recent errors from the pipeline"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    error_timestamp,
                    dag_id,
                    task_id,
                    error_type,
                    error_message,
                    severity
                FROM pipeline_error_log 
                WHERE error_timestamp >= NOW() - INTERVAL '%s hours'
                ORDER BY error_timestamp DESC
                LIMIT 50
            """, (hours,))
            
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            
            errors = []
            for row in results:
                errors.append({
                    'timestamp': row[0].isoformat() if row[0] else None,
                    'dag_id': row[1],
                    'task_id': row[2],
                    'error_type': row[3],
                    'error_message': row[4],
                    'severity': row[5]
                })
            
            return {
                'status': 'success',
                'data': errors,
                'summary': {
                    'hours_range': hours,
                    'total_errors': len(errors)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get recent errors: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'data': []
            }

# Convenience functions for direct usage
def log_pipeline_start(run_id: str, dag_id: str):
    """Convenience function to log pipeline start"""
    logger = PipelineLogger()
    logger.log_info(f"Pipeline {dag_id} started for run {run_id}")

def log_pipeline_success(run_id: str, dag_id: str):
    """Convenience function to log pipeline success"""
    logger = PipelineLogger()
    logger.log_info(f"Pipeline {dag_id} completed successfully for run {run_id}")

def log_pipeline_failure(run_id: str, dag_id: str, error: str):
    """Convenience function to log pipeline failure"""
    logger = PipelineLogger()
    logger.log_error(run_id, dag_id, 'pipeline', f"Pipeline failed: {error}")

# Main execution for testing
if __name__ == "__main__":
    # Test the logger
    logger = PipelineLogger()
    
    test_run_id = f"test_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print("Testing pipeline logger...")
    
    # Test basic logging
    logger.log_info("Testing pipeline logger functionality")
    
    # Test task logging
    logger.log_task_start(test_run_id, "test_dag", "test_task", datetime.now())
    logger.log_task_success(test_run_id, "test_dag", "test_task", 100)
    
    # Test quality check logging
    logger.log_quality_check(
        test_run_id, 
        "test_table", 
        "NULL_CHECK", 
        "PASSED", 
        0, 
        []
    )
    
    # Test error logging
    logger.log_error(
        test_run_id,
        "test_dag", 
        "test_task", 
        "Test error message",
        context_data={"test": "data"}
    )
    
    # Test health check
    health = logger.get_pipeline_health(7)
    print(f"Pipeline health: {health}")
    
    # Test recent errors
    errors = logger.get_recent_errors(24)
    print(f"Recent errors: {errors}")
    
    print("✅ Pipeline logger test completed!")