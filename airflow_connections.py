#!/usr/bin/env python3
"""
Airflow Connections Setup Script
This script creates the necessary database connections for the banking data pipeline.
"""

import os
import sys
from airflow.models import Connection
from airflow.utils.db import provide_session
from airflow import settings
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@provide_session
def create_connections(session=None):
    """Create Airflow connections for the banking pipeline"""
    
    # Connection for the main data database (postgres_data)
    postgres_data_conn = Connection(
        conn_id='postgres_data',
        conn_type='postgres',
        host='postgres_data',
        login='user',
        password='userpass',
        schema='mydata',
        port=5432,
        description='Main banking data database'
    )
    
    # Connection for Airflow metadata database
    postgres_airflow_conn = Connection(
        conn_id='postgres_default',
        conn_type='postgres',
        host='postgres',
        login='airflow',
        password='airflow',
        schema='airflow',
        port=5432,
        description='Airflow metadata database'
    )
    
    # Check if connections already exist and delete them
    existing_connections = ['postgres_data', 'postgres_default']
    
    for conn_id in existing_connections:
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if existing_conn:
            session.delete(existing_conn)
            logger.info(f"Deleted existing connection: {conn_id}")
    
    # Add new connections
    session.add(postgres_data_conn)
    session.add(postgres_airflow_conn)
    session.commit()
    
    logger.info("✅ Successfully created Airflow connections:")
    logger.info("  - postgres_data: Main banking data database")
    logger.info("  - postgres_default: Airflow metadata database")

def main():
    """Main execution function"""
    try:
        logger.info("Setting up Airflow connections for banking data pipeline...")
        create_connections()
        logger.info("Connection setup completed successfully!")
        return 0
    except Exception as e:
        logger.error(f"Failed to create connections: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())