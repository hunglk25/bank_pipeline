import random
import string
import os
import sys
import logging
from datetime import datetime, timedelta
import psycopg2
from faker import Faker

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()

class BankDataGeneratorToDB:
    def __init__(self, host="postgres_data", port=5432, database="mydata", user="user", password="userpass"):
        """Initialize the data generator with database connection parameters"""
        self.conn_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        
    def get_connection(self):
        """Create and return database connection"""
        try:
            logger.info("Connecting to database...")
            conn = psycopg2.connect(**self.conn_params)
            logger.info("Database connection established successfully")
            return conn
        except Exception as e:
            logger.info("Failed to connect to database")
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def create_tables_if_not_exist(self, conn):
        """Create tables if they don't exist"""
        cursor = conn.cursor()
        
        # Read schema from file
        try:
            schema_path = '/opt/airflow/sql/schema.sql'
            if not os.path.exists(schema_path):
                schema_path = './sql/schema.sql'
            
            if os.path.exists(schema_path):
                with open(schema_path, 'r') as f:
                    schema_sql = f.read()
                cursor.execute(schema_sql)
                conn.commit()
                logger.info("Tables created/verified successfully")
            else:
                logger.warning("Schema file not found, assuming tables exist")
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            # Continue anyway, tables might already exist
        finally:
            cursor.close()
    
    def random_password(self):
        """Generate random password"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=12))

    def random_currency(self):
        """Generate random currency"""
        return random.choice(['USD', 'VND', 'EUR'])

    def random_status(self):
        """Generate random account status"""
        return random.choice(['ACTIVE', 'FROZEN', 'CLOSED'])

    def random_account_type(self):
        """Generate random account type"""
        return random.choice(['CHECKING', 'SAVINGS', 'BUSINESS'])

    def generate_customers_to_db(self, conn, n=10):
        """Generate and insert customer data"""
        cursor = conn.cursor()
        customers = []
        
        logger.info(f"Generating {n} customers...")
        
        for i in range(n):
            try:
                national_id = fake.unique.ssn()
                name = fake.name()
                address = fake.address().replace("\n", ", ")
                contact = fake.phone_number()
                username = fake.unique.user_name()
                password_hash = self.random_password()
                
                cursor.execute("""
                    INSERT INTO Customer (NationalID, Name, Address, Contact, Username, PasswordHash)
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING CustomerID
                """, (national_id, name, address, contact, username, password_hash))
                
                customer_id = cursor.fetchone()[0]
                customers.append(customer_id)
                
            except Exception as e:
                logger.error(f"Error inserting customer {i+1}: {str(e)}")
                continue
        
        conn.commit()
        cursor.close()
        logger.info(f"Successfully generated {len(customers)} customers")
        return customers

    def generate_devices_to_db(self, conn, customers):
        """Generate and insert device data"""
        cursor = conn.cursor()
        devices = []
        
        logger.info(f"Generating devices for {len(customers)} customers...")
        
        for cust_id in customers:
            try:
                # Each customer gets 1-2 devices
                for _ in range(random.randint(1, 2)):
                    dtype = random.choice(['Phone', 'Tablet', 'Laptop'])
                    dinfo = fake.user_agent()
                    verified = random.choice([True, False])
                    last_used = fake.date_time_between(start_date='-30d', end_date='now')
                    
                    cursor.execute("""
                        INSERT INTO Device (CustomerID, DeviceType, DeviceInfo, IsVerified, LastUsed)
                        VALUES (%s, %s, %s, %s, %s) RETURNING DeviceID
                    """, (cust_id, dtype, dinfo, verified, last_used))
                    
                    device_id = cursor.fetchone()[0]
                    devices.append(device_id)
                    
            except Exception as e:
                logger.error(f"Error inserting devices for customer {cust_id}: {str(e)}")
                continue
        
        conn.commit()
        cursor.close()
        logger.info(f"Successfully generated {len(devices)} devices")
        return devices

    def generate_accounts_to_db(self, conn, customers):
        """Generate and insert account data"""
        cursor = conn.cursor()
        accounts = []
        
        logger.info(f"Generating accounts for {len(customers)} customers...")
        
        for cust_id in customers:
            try:
                # Each customer gets 1-3 accounts
                for _ in range(random.randint(1, 3)):
                    acc_type = self.random_account_type()
                    balance = round(random.uniform(1000, 1000000), 2)
                    currency = self.random_currency()
                    status = self.random_status()
                    
                    cursor.execute("""
                        INSERT INTO Account (CustomerID, AccountType, Balance, Currency, Status)
                        VALUES (%s, %s, %s, %s, %s) RETURNING AccountID
                    """, (cust_id, acc_type, balance, currency, status))
                    
                    account_id = cursor.fetchone()[0]
                    accounts.append(account_id)
                    
            except Exception as e:
                logger.error(f"Error inserting accounts for customer {cust_id}: {str(e)}")
                continue
        
        conn.commit()
        cursor.close()
        logger.info(f"Successfully generated {len(accounts)} accounts")
        return accounts

    def generate_transactions_to_db(self, conn, accounts):
        """Generate and insert transaction data"""
        if len(accounts) < 2:
            logger.warning("Not enough accounts to generate transactions")
            return
            
        cursor = conn.cursor()
        transaction_count = 0
        
        logger.info(f"Generating transactions for {len(accounts)} accounts...")
        
        for _ in range(50):  # Generate 50 transactions
            try:
                from_acc, to_acc = random.sample(accounts, 2)
                amount = round(random.uniform(100, 50000), 2)
                txn_type = random.choice(['TRANSFER', 'PAYMENT'])
                ts = fake.date_time_between(start_date='-10d', end_date='now')
                risk = amount > 20000  # Flag high-value transactions as risky
                
                cursor.execute("""
                    INSERT INTO Transaction (FromAccountID, ToAccountID, TxnType, Amount, Timestamp, RiskFlag)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (from_acc, to_acc, txn_type, amount, ts, risk))
                
                transaction_count += 1
                
            except Exception as e:
                logger.error(f"Error inserting transaction: {str(e)}")
                continue
        
        conn.commit()
        cursor.close()
        logger.info(f"Successfully generated {transaction_count} transactions")

    def generate_auth_logs_to_db(self, conn, customers, devices):
        """Generate and insert authentication log data"""
        if not customers or not devices:
            logger.warning("No customers or devices available for auth logs")
            return
            
        cursor = conn.cursor()
        auth_log_count = 0
        
        logger.info(f"Generating authentication logs...")
        
        for _ in range(30):  # Generate 30 auth logs
            try:
                cust_id = random.choice(customers)
                device_id = random.choice(devices)
                method = random.choice(['OTP', 'Biometric', 'Password'])
                status = random.choice(['SUCCESS', 'FAIL'])
                ts = fake.date_time_between(start_date='-7d', end_date='now')
                
                cursor.execute("""
                    INSERT INTO AuthenticationLog (CustomerID, DeviceID, AuthMethod, AuthStatus, Timestamp)
                    VALUES (%s, %s, %s, %s, %s)
                """, (cust_id, device_id, method, status, ts))
                
                auth_log_count += 1
                
            except Exception as e:
                logger.error(f"Error inserting auth log: {str(e)}")
                continue
        
        conn.commit()
        cursor.close()
        logger.info(f"Successfully generated {auth_log_count} authentication logs")

    def generate_all_data_to_db(self, customer_count=10):
        """Generate all types of data"""
        logger.info("Starting data generation process...")
        conn = self.get_connection()

        logger.info(f"Generating data for {customer_count} customers...")
        try:
            # Create tables if they don't exist
            self.create_tables_if_not_exist(conn)
            
            # Generate data in sequence
            customers = self.generate_customers_to_db(conn, customer_count)
            if not customers:
                raise Exception("Failed to generate customers")
                
            devices = self.generate_devices_to_db(conn, customers)
            accounts = self.generate_accounts_to_db(conn, customers)
            
            if accounts:
                self.generate_transactions_to_db(conn, accounts)
            
            if customers and devices:
                self.generate_auth_logs_to_db(conn, customers, devices)
            return True
            
        except Exception as e:
            logger.error(f"❌ Data generation failed: {str(e)}")
            raise
        finally:
            conn.close()

def main():
    """Main function for standalone execution"""
    try:
        # Get customer count from environment variable or use default
        customer_count = int(os.getenv('CUSTOMER_COUNT', '10'))
        
        generator = BankDataGeneratorToDB()
        success = generator.generate_all_data_to_db(customer_count)
        
        if success:
            logger.info("Data generation process completed successfully")
            sys.exit(0)
        else:
            logger.error("Data generation process failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error in data generation: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
