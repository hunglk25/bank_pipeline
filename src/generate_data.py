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
        used_ids = set()
        
        for i in range(n):
            # Sometimes generate duplicate IDs (20% chance)
            if random.random() < 0.2 and used_ids:
                customer_id = random.choice(list(used_ids))
            else:
                # Generate random ID between 1000 and 999999
                customer_id = random.randint(1000, 999999)
                # Ensure uniqueness for new IDs
                while customer_id in used_ids:
                    customer_id = random.randint(1000, 999999)
            
            used_ids.add(customer_id)
            
            customers.append({
                'CustomerID': customer_id,
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
        used_ids = set()
        
        for customer in customers_data:
            # Each customer has 1-3 devices
            num_devices = random.randint(1, 3)
            for _ in range(num_devices):
                # Sometimes generate duplicate IDs (15% chance)
                if random.random() < 0.15 and used_ids:
                    device_id = random.choice(list(used_ids))
                else:
                    # Generate unique device ID between 10000 and 9999999
                    device_id = random.randint(10000, 9999999)
                    while device_id in used_ids:
                        device_id = random.randint(10000, 9999999)
                
                used_ids.add(device_id)
                
                devices.append({
                    'DeviceID': device_id,
                    'CustomerID': customer['CustomerID'],
                    'DeviceType': random.choice(['Phone', 'Tablet', 'Laptop']),
                    'DeviceInfo': fake.user_agent(),
                    'IsVerified': random.choice([True, False]),
                    'LastUsed': fake.date_time_between(start_date='-30d', end_date='now')
                })
        logger.info(f"Generated {len(devices)} devices")
        return devices

    def generate_accounts(self, customers_data):
        logger.info("Generating accounts...")
        accounts = []
        used_ids = set()
        
        for customer in customers_data:
            for _ in range(random.randint(1, 3)):
                # Sometimes generate duplicate IDs (10% chance)
                if random.random() < 0.1 and used_ids:
                    account_id = random.choice(list(used_ids))
                else:
                    # Generate random ID between 100000 and 99999999
                    account_id = random.randint(100000, 99999999)
                    while account_id in used_ids:
                        account_id = random.randint(100000, 99999999)
                
                used_ids.add(account_id)
                
                accounts.append({
                    'AccountID': account_id,
                    'CustomerID': customer['CustomerID'],
                    'AccountType': random.choice(['CHECKING', 'SAVINGS', 'BUSINESS']),
                    'Balance': round(random.uniform(1000, 1000000), 2),
                    'Currency': random.choice(['USD', 'VND', 'EUR']),
                    'Status': random.choice(['ACTIVE', 'FROZEN', 'CLOSED'])
                })
        logger.info(f"Generated {len(accounts)} accounts")
        return accounts

    def generate_transactions(self, accounts_data, device_data):
        logger.info("Generating transactions...")
        if len(accounts_data) < 2:
            return []
        
        # Create customer to devices mapping for efficient lookup
        customer_devices = {}
        for device in device_data:
            customer_id = device['CustomerID']
            if customer_id not in customer_devices:
                customer_devices[customer_id] = []
            customer_devices[customer_id].append(device)
        
        transactions = []
        used_ids = set()
        
        for i in range(50):
            # Sometimes generate duplicate IDs (25% chance)
            if random.random() < 0.25 and used_ids:
                transaction_id = random.choice(list(used_ids))
            else:
                # Generate random ID between 1000000 and 999999999
                transaction_id = random.randint(1000000, 999999999)
                while transaction_id in used_ids:
                    transaction_id = random.randint(1000000, 999999999)
            
            used_ids.add(transaction_id)
            
            from_acc = random.choice(accounts_data)
            to_acc = random.choice([acc for acc in accounts_data if acc['AccountID'] != from_acc['AccountID']])
            
            # Ensure device belongs to the same customer as the from_account
            from_customer_id = from_acc['CustomerID']
            if from_customer_id in customer_devices and customer_devices[from_customer_id]:
                device = random.choice(customer_devices[from_customer_id])
            else:
                # Fallback if no devices for this customer (shouldn't happen with proper data)
                device = random.choice(device_data)
            
            amount = round(random.uniform(20000000, 100000000), 2)
            
            transactions.append({
                'TransactionID': transaction_id,
                'FromAccountID': from_acc['AccountID'],
                'ToAccountID': to_acc['AccountID'],
                'DeviceID': device['DeviceID'],
                'TxnType': random.choice(['TRANSFER', 'PAYMENT']),
                'Amount': amount,
                'Timestamp': fake.date_time_between(start_date='-10d', end_date='now'),
            })
        logger.info(f"Generated {len(transactions)} transactions")
        return transactions

    def generate_auth_logs(self, customers_data, devices_data):
        logger.info("Generating auth logs...")
        if not customers_data or not devices_data:
            return []
        
        # Create customer to devices mapping for efficient lookup
        customer_devices = {}
        for device in devices_data:
            customer_id = device['CustomerID']
            if customer_id not in customer_devices:
                customer_devices[customer_id] = []
            customer_devices[customer_id].append(device)
        
        auth_logs = []
        used_ids = set()
        
        for i in range(30):
            # Sometimes generate duplicate IDs (30% chance)
            if random.random() < 0.3 and used_ids:
                auth_id = random.choice(list(used_ids))
            else:
                # Generate random ID between 10000 and 9999999
                auth_id = random.randint(10000, 9999999)
                while auth_id in used_ids:
                    auth_id = random.randint(10000, 9999999)
            
            used_ids.add(auth_id)
            
            # Select a random customer and one of their devices
            customer = random.choice(customers_data)
            customer_id = customer['CustomerID']
            
            if customer_id in customer_devices and customer_devices[customer_id]:
                device = random.choice(customer_devices[customer_id])
            else:
                # Fallback if no devices for this customer (shouldn't happen with proper data)
                device = random.choice(devices_data)
                customer_id = device['CustomerID']
            
            auth_logs.append({
                'AuthID': auth_id,
                'CustomerID': customer_id,
                'DeviceID': device['DeviceID'],
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
        transactions = self.generate_transactions(accounts, devices)
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
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    main()