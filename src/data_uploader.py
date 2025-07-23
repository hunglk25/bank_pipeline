import psycopg2
import logging
import json
import sys
import os
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataUploader:
    def __init__(self):
        self.conn_params = {
            'host': "postgres_data", 'port': 5432, 'database': "mydata", 
            'user': "user", 'password': "userpass"
        }
    def get_connection(self):
        try:
            conn = psycopg2.connect(**self.conn_params)
            logger.info("Database connected")
            return conn
        except Exception as e:
            logger.error(f"DB connection failed: {str(e)}")
            raise
    
    def create_tables_if_needed(self, conn):
        cursor = conn.cursor()
        try:
            schema_paths = ['/opt/airflow/sql/schema.sql', './sql/schema.sql']
            for path in schema_paths:
                if os.path.exists(path):
                    with open(path, 'r') as f:
                        cursor.execute(f.read())
                    conn.commit()
                    logger.info("Tables created/verified")
                    break
        except Exception as e:
            logger.warning(f"Error creating tables: {str(e)}")
        finally:
            cursor.close()
    def upload_data(self, conn, table_name, data, columns, insert_sql):
        if not data:
            return 0
        cursor = conn.cursor()
        count = 0
        try:
            for record in data:
                values = [record.get(col) for col in columns]
                cursor.execute(insert_sql, values)
                count += cursor.rowcount
            conn.commit()
            logger.info(f"Uploaded {count} {table_name}")
            return count
        except Exception as e:
            conn.rollback()
            logger.error(f"Error uploading {table_name}: {str(e)}")
            raise
        finally:
            cursor.close()

    def upload_all_data(self, conn, data):
        configs = {
            'customers': {
                'columns': ['CustomerID', 'NationalID', 'Name', 'Address', 'Contact', 'Username', 'PasswordHash'],
                'sql': """INSERT INTO Customer (CustomerID, NationalID, Name, Address, Contact, Username, PasswordHash)
                         VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (Username) DO NOTHING"""
            },
            'devices': {
                'columns': ['DeviceID', 'CustomerID', 'DeviceType', 'DeviceInfo', 'IsVerified', 'LastUsed'],
                'sql': """INSERT INTO Device (DeviceID, CustomerID, DeviceType, DeviceInfo, IsVerified, LastUsed)
                         VALUES (%s, %s, %s, %s, %s, %s)"""
            },
            'accounts': {
                'columns': ['AccountID', 'CustomerID', 'AccountType', 'Balance', 'Currency', 'Status'],
                'sql': """INSERT INTO Account (AccountID, CustomerID, AccountType, Balance, Currency, Status)
                         VALUES (%s, %s, %s, %s, %s, %s)"""
            },
            'transactions': {
                'columns': ['TransactionID', 'FromAccountID', 'ToAccountID', 'TxnType', 'Amount', 'Timestamp', 'RiskFlag'],
                'sql': """INSERT INTO Transaction (TransactionID, FromAccountID, ToAccountID, TxnType, Amount, Timestamp, RiskFlag)
                         VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            },
            'auth_logs': {
                'columns': ['AuthID', 'CustomerID', 'DeviceID', 'AuthMethod', 'AuthStatus', 'Timestamp'],
                'sql': """INSERT INTO AuthenticationLog (AuthID, CustomerID, DeviceID, AuthMethod, AuthStatus, Timestamp)
                         VALUES (%s, %s, %s, %s, %s, %s)"""
            },
           'risk_alerts': {
               'columns': ['CustomerID', 'TransactionID', 'alert_type', 'alert_level', 'description', 'timestamp'],
               'sql': """INSERT INTO RiskAlerts (CustomerID, TransactionID, AlertType, AlertLevel, Description, CreatedAt)
                        VALUES (%s, %s, %s, %s, %s, %s)"""
           }
        }
        
        total = 0
        for table_name, config in configs.items():
            if table_name in data:
                total += self.upload_data(conn, table_name, data[table_name], 
                                        config['columns'], config['sql'])
        return total

def load_json_data(data_dir):
    data = {}
    if not os.path.exists(data_dir):
        logger.error(f"Directory not found: {data_dir}")
        return data
    
    json_files = ['customers.json', 'devices.json', 'accounts.json', 'transactions.json', 'auth_logs.json', 'risk_alerts.json']
    for filename in json_files:
        filepath = os.path.join(data_dir, filename)
        table_name = filename.replace('.json', '')
        try:
            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    data[table_name] = json.load(f)
                logger.info(f"Loaded {len(data[table_name])} {table_name}")

                os.remove(filepath)
                logger.info(f"Deleted file: {filepath}")
            else:
                data[table_name] = []
        except Exception as e:
            logger.error(f"Error loading {filename}: {str(e)}")
            data[table_name] = []
    return data

def main():
    parser = argparse.ArgumentParser(description="Upload clean JSON data to database")
    parser.add_argument('--dir', required=True, help='Directory containing JSON files')
    args = parser.parse_args()
    
    try:
        data = load_json_data(args.dir)
        if not any(data.values()):
            logger.info("No data found to upload")
            return
        
        uploader = DataUploader()
        conn = uploader.get_connection()
        
        try:
            uploader.create_tables_if_needed(conn)
            total_uploaded = uploader.upload_all_data(conn, data)
            
            if total_uploaded > 0:
                logger.info(f"Upload completed - {total_uploaded} total records")
                print(f"Upload successful - {total_uploaded} records uploaded")
            else:
                logger.warning("No records were uploaded")
                print("Upload completed but no records were inserted")
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        print(f"Upload failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
