#!/usr/bin/env python3
"""
Data Uploader Module
Handles uploading validated data to PostgreSQL with error handling and logging.
"""

import psycopg2
import logging
import json
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataUploader:
    def __init__(self, host="postgres_data", port=5432, database="mydata", user="user", password="userpass"):
        """Initialize the data uploader with database connection parameters"""
        self.conn_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        self.upload_errors = []
        
    def get_connection(self):
        """Create and return database connection"""
        try:
            conn = psycopg2.connect(**self.conn_params)
            logger.info("Database connection established successfully")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def create_staging_tables(self, conn):
        """Create staging tables for data processing"""
        cursor = conn.cursor()
        
        try:
            # Create staging schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")
            
            # Create staging tables (simplified versions of main tables)
            staging_tables = """
            -- Staging Customer table
            CREATE TABLE IF NOT EXISTS staging.customers (
                customer_id SERIAL PRIMARY KEY,
                national_id VARCHAR(50),
                name VARCHAR(100),
                address TEXT,
                contact VARCHAR(50),
                username VARCHAR(50),
                password_hash VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW(),
                batch_id VARCHAR(50)
            );
            
            -- Staging Account table
            CREATE TABLE IF NOT EXISTS staging.accounts (
                account_id SERIAL PRIMARY KEY,
                customer_id INTEGER,
                account_type VARCHAR(20),
                balance DECIMAL(15,2),
                currency VARCHAR(3),
                status VARCHAR(10),
                created_at TIMESTAMP DEFAULT NOW(),
                batch_id VARCHAR(50)
            );
            
            -- Staging Transaction table
            CREATE TABLE IF NOT EXISTS staging.transactions (
                transaction_id SERIAL PRIMARY KEY,
                from_account_id INTEGER,
                to_account_id INTEGER,
                txn_type VARCHAR(20),
                amount DECIMAL(15,2),
                timestamp TIMESTAMP,
                risk_flag BOOLEAN,
                created_at TIMESTAMP DEFAULT NOW(),
                batch_id VARCHAR(50)
            );
            
            -- Upload log table
            CREATE TABLE IF NOT EXISTS upload_log (
                log_id SERIAL PRIMARY KEY,
                batch_id VARCHAR(50),
                upload_timestamp TIMESTAMP DEFAULT NOW(),
                status VARCHAR(20),
                records_processed INTEGER,
                records_successful INTEGER,
                records_failed INTEGER,
                error_details TEXT
            );
            """
            
            cursor.execute(staging_tables)
            conn.commit()
            logger.info("Staging tables created/verified successfully")
            
        except Exception as e:
            logger.error(f"Error creating staging tables: {str(e)}")
            raise
        finally:
            cursor.close()
    
    def generate_batch_id(self):
        """Generate a unique batch ID for this upload session"""
        return f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def upload_to_staging(self, conn, table_name: str, data: List[Dict], batch_id: str):
        """Upload data to staging tables"""
        if not data:
            logger.warning(f"No data to upload for table: {table_name}")
            return 0, 0
            
        cursor = conn.cursor()
        successful_records = 0
        failed_records = 0
        
        try:
            for record in data:
                try:
                    # Add batch_id to record
                    record['batch_id'] = batch_id
                    
                    # Dynamic SQL generation based on table
                    if table_name == 'customers':
                        cursor.execute("""
                            INSERT INTO staging.customers 
                            (national_id, name, address, contact, username, password_hash, batch_id)
                            VALUES (%(national_id)s, %(name)s, %(address)s, %(contact)s, %(username)s, %(password_hash)s, %(batch_id)s)
                        """, record)
                    elif table_name == 'accounts':
                        cursor.execute("""
                            INSERT INTO staging.accounts 
                            (customer_id, account_type, balance, currency, status, batch_id)
                            VALUES (%(customer_id)s, %(account_type)s, %(balance)s, %(currency)s, %(status)s, %(batch_id)s)
                        """, record)
                    elif table_name == 'transactions':
                        cursor.execute("""
                            INSERT INTO staging.transactions 
                            (from_account_id, to_account_id, txn_type, amount, timestamp, risk_flag, batch_id)
                            VALUES (%(from_account_id)s, %(to_account_id)s, %(txn_type)s, %(amount)s, %(timestamp)s, %(risk_flag)s, %(batch_id)s)
                        """, record)
                    
                    successful_records += 1
                    
                except Exception as e:
                    failed_records += 1
                    error_msg = f"Failed to insert record into {table_name}: {str(e)}"
                    self.upload_errors.append(error_msg)
                    logger.error(error_msg)
            
            conn.commit()
            logger.info(f"Uploaded to staging.{table_name}: {successful_records} successful, {failed_records} failed")
            
        except Exception as e:
            conn.rollback()
            error_msg = f"Critical error uploading to {table_name}: {str(e)}"
            self.upload_errors.append(error_msg)
            logger.error(error_msg)
            raise
        finally:
            cursor.close()
            
        return successful_records, failed_records
    
    def promote_to_production(self, conn, batch_id: str):
        """Move validated data from staging to production tables"""
        cursor = conn.cursor()
        
        try:
            # Promote customers
            cursor.execute("""
                INSERT INTO Customer (NationalID, Name, Address, Contact, Username, PasswordHash)
                SELECT national_id, name, address, contact, username, password_hash
                FROM staging.customers 
                WHERE batch_id = %s
                ON CONFLICT (Username) DO NOTHING
            """, (batch_id,))
            customers_promoted = cursor.rowcount
            
            # Promote accounts (need to map customer IDs)
            cursor.execute("""
                INSERT INTO Account (CustomerID, AccountType, Balance, Currency, Status)
                SELECT c.CustomerID, sa.account_type, sa.balance, sa.currency, sa.status
                FROM staging.accounts sa
                JOIN staging.customers sc ON sa.customer_id = sc.customer_id
                JOIN Customer c ON c.Username = sc.username
                WHERE sa.batch_id = %s
            """, (batch_id,))
            accounts_promoted = cursor.rowcount
            
            # Promote transactions (need to map account IDs)
            cursor.execute("""
                INSERT INTO Transaction (FromAccountID, ToAccountID, TxnType, Amount, Timestamp, RiskFlag)
                SELECT a1.AccountID, a2.AccountID, st.txn_type, st.amount, st.timestamp, st.risk_flag
                FROM staging.transactions st
                JOIN staging.accounts sa1 ON st.from_account_id = sa1.account_id
                JOIN staging.accounts sa2 ON st.to_account_id = sa2.account_id
                JOIN staging.customers sc1 ON sa1.customer_id = sc1.customer_id
                JOIN staging.customers sc2 ON sa2.customer_id = sc2.customer_id
                JOIN Customer c1 ON c1.Username = sc1.username
                JOIN Customer c2 ON c2.Username = sc2.username
                JOIN Account a1 ON a1.CustomerID = c1.CustomerID AND a1.AccountType = sa1.account_type
                JOIN Account a2 ON a2.CustomerID = c2.CustomerID AND a2.AccountType = sa2.account_type
                WHERE st.batch_id = %s
            """, (batch_id,))
            transactions_promoted = cursor.rowcount
            
            conn.commit()
            
            logger.info(f"Successfully promoted to production:")
            logger.info(f"  - Customers: {customers_promoted}")
            logger.info(f"  - Accounts: {accounts_promoted}")
            logger.info(f"  - Transactions: {transactions_promoted}")
            
            return {
                'customers': customers_promoted,
                'accounts': accounts_promoted,
                'transactions': transactions_promoted
            }
            
        except Exception as e:
            conn.rollback()
            error_msg = f"Error promoting data to production: {str(e)}"
            self.upload_errors.append(error_msg)
            logger.error(error_msg)
            raise
        finally:
            cursor.close()
    
    def log_upload_result(self, conn, batch_id: str, status: str, stats: Dict):
        """Log the upload results"""
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO upload_log (batch_id, status, records_processed, records_successful, records_failed, error_details)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                batch_id,
                status,
                stats.get('processed', 0),
                stats.get('successful', 0),
                stats.get('failed', 0),
                json.dumps(self.upload_errors) if self.upload_errors else None
            ))
            conn.commit()
            logger.info(f"Upload result logged for batch: {batch_id}")
            
        except Exception as e:
            logger.error(f"Error logging upload result: {str(e)}")
        finally:
            cursor.close()
    
    def cleanup_staging_data(self, conn, batch_id: str):
        """Clean up staging data after successful promotion"""
        cursor = conn.cursor()
        
        try:
            cursor.execute("DELETE FROM staging.customers WHERE batch_id = %s", (batch_id,))
            cursor.execute("DELETE FROM staging.accounts WHERE batch_id = %s", (batch_id,))
            cursor.execute("DELETE FROM staging.transactions WHERE batch_id = %s", (batch_id,))
            conn.commit()
            logger.info(f"Cleaned up staging data for batch: {batch_id}")
            
        except Exception as e:
            logger.error(f"Error cleaning up staging data: {str(e)}")
        finally:
            cursor.close()
    
    def upload_data(self, customers_data: List[Dict] = None, accounts_data: List[Dict] = None, 
                   transactions_data: List[Dict] = None):
        """Main upload function"""
        logger.info("Starting data upload process...")
        
        batch_id = self.generate_batch_id()
        conn = self.get_connection()
        
        try:
            # Create staging tables
            self.create_staging_tables(conn)
            
            total_processed = 0
            total_successful = 0
            total_failed = 0
            
            # Upload to staging tables
            if customers_data:
                success, failed = self.upload_to_staging(conn, 'customers', customers_data, batch_id)
                total_processed += len(customers_data)
                total_successful += success
                total_failed += failed
            
            if accounts_data:
                success, failed = self.upload_to_staging(conn, 'accounts', accounts_data, batch_id)
                total_processed += len(accounts_data)
                total_successful += success
                total_failed += failed
            
            if transactions_data:
                success, failed = self.upload_to_staging(conn, 'transactions', transactions_data, batch_id)
                total_processed += len(transactions_data)
                total_successful += success
                total_failed += failed
            
            # If we have critical errors, don't promote to production
            if self.upload_errors:
                status = 'FAILED'
                logger.error(f"Upload failed with {len(self.upload_errors)} errors")
            else:
                # Promote to production
                promotion_stats = self.promote_to_production(conn, batch_id)
                self.cleanup_staging_data(conn, batch_id)
                status = 'SUCCESS'
                logger.info("✅ Data upload completed successfully")
            
            # Log results
            stats = {
                'processed': total_processed,
                'successful': total_successful,
                'failed': total_failed
            }
            self.log_upload_result(conn, batch_id, status, stats)
            
            return status == 'SUCCESS'
            
        except Exception as e:
            logger.error(f"❌ Data upload failed: {str(e)}")
            self.upload_errors.append(str(e))
            
            # Log failure
            stats = {'processed': 0, 'successful': 0, 'failed': 1}
            self.log_upload_result(conn, batch_id, 'FAILED', stats)
            
            raise
        finally:
            conn.close()

def main():
    """Main function for standalone testing"""
    try:
        uploader = DataUploader()
        
        # Sample data for testing
        sample_customers = [
            {
                'national_id': '123456789',
                'name': 'Test User',
                'address': '123 Test St',
                'contact': '555-0123',
                'username': 'testuser',
                'password_hash': 'testhash123'
            }
        ]
        
        success = uploader.upload_data(customers_data=sample_customers)
        
        if success:
            logger.info("Test upload completed successfully")
            sys.exit(0)
        else:
            logger.error("Test upload failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error in data upload: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
