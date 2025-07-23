import random
import string
import os
import json
import argparse
import logging
from datetime import datetime
from faker import Faker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
fake = Faker()

def convert_datetimes(obj):
    if isinstance(obj, list):
        return [convert_datetimes(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_datetimes(value) for key, value in obj.items()}
    elif isinstance(obj, datetime):
        return obj.isoformat()
    return obj

class BankDataGenerator:
    def generate_customers(self, n=10):
        logger.info(f"Generating {n} customers...")
        customers = []
        for i in range(n):
            customers.append({
                'CustomerID': i + 1,
                'NationalID': ''.join(random.choices('0123456789', k=12)),
                'Name': fake.name(),
                'Address': fake.address().replace("\n", ", "),
                'Contact': fake.phone_number(),
                'Username': fake.unique.user_name(),
                'PasswordHash': ''.join(random.choices(string.ascii_letters + string.digits, k=12))
            })
        logger.info(f"Generated {len(customers)} customers")
        return customers

    def generate_devices(self, customers_data):
        logger.info("Generating devices...")
        devices = []
        device_id = 1
        for customer in customers_data:
            for _ in range(random.randint(1, 2)):
                devices.append({
                    'DeviceID': device_id,
                    'CustomerID': customer['CustomerID'],
                    'DeviceType': random.choice(['Phone', 'Tablet', 'Laptop']),
                    'DeviceInfo': fake.user_agent(),
                    'IsVerified': random.choice([True, False]),
                    'LastUsed': fake.date_time_between(start_date='-30d', end_date='now')
                })
                device_id += 1
        logger.info(f"Generated {len(devices)} devices")
        return devices

    def generate_accounts(self, customers_data):
        logger.info("Generating accounts...")
        accounts = []
        account_id = 1
        for customer in customers_data:
            for _ in range(random.randint(1, 3)):
                accounts.append({
                    'AccountID': account_id,
                    'CustomerID': customer['CustomerID'],
                    'AccountType': random.choice(['CHECKING', 'SAVINGS', 'BUSINESS']),
                    'Balance': round(random.uniform(1000, 1000000), 2),
                    'Currency': random.choice(['USD', 'VND', 'EUR']),
                    'Status': random.choice(['ACTIVE', 'FROZEN', 'CLOSED'])
                })
                account_id += 1
        logger.info(f"Generated {len(accounts)} accounts")
        return accounts

    def generate_transactions(self, accounts_data):
        logger.info("Generating transactions...")
        if len(accounts_data) < 2:
            return []
        
        transactions = []
        for i in range(50):
            from_acc = random.choice(accounts_data)
            to_acc = random.choice([acc for acc in accounts_data if acc['AccountID'] != from_acc['AccountID']])
            amount = round(random.uniform(10000000, 90000000), 2)
            
            transactions.append({
                'TransactionID': i + 1,
                'FromAccountID': from_acc['AccountID'],
                'ToAccountID': to_acc['AccountID'],
                'TxnType': random.choice(['TRANSFER', 'PAYMENT']),
                'Amount': amount,
                'Timestamp': fake.date_time_between(start_date='-10d', end_date='now'),
                'RiskFlag': amount > 20000
            })
        logger.info(f"Generated {len(transactions)} transactions")
        return transactions

    def generate_auth_logs(self, customers_data, devices_data):
        logger.info("Generating auth logs...")
        if not customers_data or not devices_data:
            return []
        
        auth_logs = []
        for i in range(30):
            auth_logs.append({
                'AuthID': i + 1,
                'CustomerID': random.choice(customers_data)['CustomerID'],
                'DeviceID': random.choice(devices_data)['DeviceID'],
                'AuthMethod': random.choice(['OTP', 'Biometric', 'Password']),
                'AuthStatus': random.choice(['SUCCESS', 'FAIL']),
                'Timestamp': fake.date_time_between(start_date='-7d', end_date='now')
            })
        logger.info(f"Generated {len(auth_logs)} auth logs")
        return auth_logs

    def generate_all_data(self, customer_count=10):
        logger.info(f"Starting data generation for {customer_count} customers...")
        
        customers = self.generate_customers(customer_count)
        devices = self.generate_devices(customers)
        accounts = self.generate_accounts(customers)
        transactions = self.generate_transactions(accounts)
        auth_logs = self.generate_auth_logs(customers, devices)
        
        data = {
            'customers': customers,
            'devices': devices,
            'accounts': accounts,
            'transactions': transactions,
            'auth_logs': auth_logs
        }
        
        total_records = sum(len(v) for v in data.values())
        logger.info(f"Data generation completed - {total_records} total records")
        return data

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic banking data")
    parser.add_argument('--output_dir', required=True, help='Directory to save generated data')
    args = parser.parse_args()
    
    try:
        os.makedirs(args.output_dir, exist_ok=True)
        customer_count = int(os.getenv('CUSTOMER_COUNT', '10'))
        
        generator = BankDataGenerator()
        data = generator.generate_all_data(customer_count)
        
        if not data:
            logger.error("Data generation failed")
            return 1
        
        # Save to JSON files
        for key, records in data.items():
            filepath = os.path.join(args.output_dir, f"{key}.json")
            with open(filepath, 'w') as f:
                json.dump(convert_datetimes(records), f, indent=2)
            logger.info(f"Saved {len(records)} {key} to {filepath}")
        
        # Summary
        total = sum(len(v) for v in data.values())
        logger.info(f"Summary: {total} records saved to {args.output_dir}")
        print(args.output_dir)  # For Airflow to capture
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    main()