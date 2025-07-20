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
                'NationalID': fake.unique.ssn(),
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
            amount = round(random.uniform(100, 50000), 2)
            
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
                'AuthLogID': i + 1,
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








# import random
# import string
# import os
# import sys
# import logging
# from datetime import datetime, timedelta
# import json
# import argparse
# from faker import Faker

# # Setup logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# fake = Faker()
# def convert_datetimes(obj):
#     if isinstance(obj, list):
#         return [convert_datetimes(item) for item in obj]
#     elif isinstance(obj, dict):
#         return {key: convert_datetimes(value) for key, value in obj.items()}
#     elif isinstance(obj, datetime):
#         return obj.isoformat()
#     else:
#         return obj

# class BankDataGenerator:
#     def __init__(self):
#         """Initialize the data generator"""
#         pass
        
#     def random_password(self):
#         """Generate random password"""
#         return ''.join(random.choices(string.ascii_letters + string.digits, k=12))

#     def random_currency(self):
#         """Generate random currency"""
#         return random.choice(['USD', 'VND', 'EUR'])

#     def random_status(self):
#         """Generate random account status"""
#         return random.choice(['ACTIVE', 'FROZEN', 'CLOSED'])

#     def random_account_type(self):
#         """Generate random account type"""
#         return random.choice(['CHECKING', 'SAVINGS', 'BUSINESS'])

#     def generate_customers(self, n=10):
#         """Generate customer data and return as list"""
#         customers_data = []
        
#         logger.info(f"Generating {n} customers...")
        
#         for i in range(n):
#             try:
#                 customer_id = i + 1
#                 national_id = fake.unique.ssn()
#                 name = fake.name()
#                 address = fake.address().replace("\n", ", ")
#                 contact = fake.phone_number()
#                 username = fake.unique.user_name()
#                 password_hash = self.random_password()
                
#                 customer_data = {
#                     'CustomerID': customer_id,
#                     'NationalID': national_id,
#                     'Name': name,
#                     'Address': address,
#                     'Contact': contact,
#                     'Username': username,
#                     'PasswordHash': password_hash
#                 }
                
#                 customers_data.append(customer_data)
                
#             except Exception as e:
#                 logger.error(f"Error generating customer {i+1}: {str(e)}")
#                 continue
        
#         logger.info(f"Successfully generated {len(customers_data)} customers")
#         return customers_data

#     def generate_devices(self, customers_data):
#         """Generate device data and return as list"""
#         devices_data = []
#         device_id_counter = 1
        
#         logger.info(f"Generating devices for {len(customers_data)} customers...")
        
#         for customer in customers_data:
#             cust_id = customer['CustomerID']
#             try:
#                 # Each customer gets 1-2 devices
#                 for _ in range(random.randint(1, 2)):
#                     dtype = random.choice(['Phone', 'Tablet', 'Laptop'])
#                     dinfo = fake.user_agent()
#                     verified = random.choice([True, False])
#                     last_used = fake.date_time_between(start_date='-30d', end_date='now')
                    
#                     device_data = {
#                         'DeviceID': device_id_counter,
#                         'CustomerID': cust_id,
#                         'DeviceType': dtype,
#                         'DeviceInfo': dinfo,
#                         'IsVerified': verified,
#                         'LastUsed': last_used
#                     }
                    
#                     devices_data.append(device_data)
#                     device_id_counter += 1
                    
#             except Exception as e:
#                 logger.error(f"Error generating devices for customer {cust_id}: {str(e)}")
#                 continue
        
#         logger.info(f"Successfully generated {len(devices_data)} devices")
#         return devices_data

#     def generate_accounts(self, customers_data):
#         """Generate account data and return as list"""
#         accounts_data = []
#         account_id_counter = 1
        
#         logger.info(f"Generating accounts for {len(customers_data)} customers...")
        
#         for customer in customers_data:
#             cust_id = customer['CustomerID']
#             try:
#                 # Each customer gets 1-3 accounts
#                 for _ in range(random.randint(1, 3)):
#                     acc_type = self.random_account_type()
#                     balance = round(random.uniform(1000, 1000000), 2)
#                     currency = self.random_currency()
#                     status = self.random_status()
                    
#                     account_data = {
#                         'AccountID': account_id_counter,
#                         'CustomerID': cust_id,
#                         'AccountType': acc_type,
#                         'Balance': balance,
#                         'Currency': currency,
#                         'Status': status
#                     }
                    
#                     accounts_data.append(account_data)
#                     account_id_counter += 1
                    
#             except Exception as e:
#                 logger.error(f"Error generating accounts for customer {cust_id}: {str(e)}")
#                 continue
        
#         logger.info(f"Successfully generated {len(accounts_data)} accounts")
#         return accounts_data

#     def generate_transactions(self, accounts_data):
#         """Generate transaction data and return as list"""
#         if len(accounts_data) < 2:
#             logger.warning("Not enough accounts to generate transactions")
#             return []
            
#         transactions_data = []
#         transaction_id_counter = 1
        
#         logger.info(f"Generating transactions for {len(accounts_data)} accounts...")
        
#         for _ in range(50):  # Generate 50 transactions
#             try:
#                 from_acc = random.choice(accounts_data)
#                 to_acc = random.choice(accounts_data)
                
#                 # Make sure from and to accounts are different
#                 while to_acc['AccountID'] == from_acc['AccountID']:
#                     to_acc = random.choice(accounts_data)
                
#                 amount = round(random.uniform(100, 50000), 2)
#                 txn_type = random.choice(['TRANSFER', 'PAYMENT'])
#                 ts = fake.date_time_between(start_date='-10d', end_date='now')
#                 risk = amount > 20000  # Flag high-value transactions as risky
                
#                 transaction_data = {
#                     'TransactionID': transaction_id_counter,
#                     'FromAccountID': from_acc['AccountID'],
#                     'ToAccountID': to_acc['AccountID'],
#                     'TxnType': txn_type,
#                     'Amount': amount,
#                     'Timestamp': ts,
#                     'RiskFlag': risk
#                 }
                
#                 transactions_data.append(transaction_data)
#                 transaction_id_counter += 1
                
#             except Exception as e:
#                 logger.error(f"Error generating transaction: {str(e)}")
#                 continue
        
#         logger.info(f"Successfully generated {len(transactions_data)} transactions")
#         return transactions_data
    

#     def generate_auth_logs(self, customers_data, devices_data):
#         """Generate authentication log data and return as list"""
#         if not customers_data or not devices_data:
#             logger.warning("No customers or devices available for auth logs")
#             return []
            
#         auth_logs_data = []
#         auth_log_id_counter = 1
        
#         logger.info(f"Generating authentication logs...")
        
#         for _ in range(30):  # Generate 30 auth logs
#             try:
#                 customer = random.choice(customers_data)
#                 device = random.choice(devices_data)
#                 method = random.choice(['OTP', 'Biometric', 'Password'])
#                 status = random.choice(['SUCCESS', 'FAIL'])
#                 ts = fake.date_time_between(start_date='-7d', end_date='now')
                
#                 auth_log_data = {
#                     'AuthLogID': auth_log_id_counter,
#                     'CustomerID': customer['CustomerID'],
#                     'DeviceID': device['DeviceID'],
#                     'AuthMethod': method,
#                     'AuthStatus': status,
#                     'Timestamp': ts
#                 }
                
#                 auth_logs_data.append(auth_log_data)
#                 auth_log_id_counter += 1
                
#             except Exception as e:
#                 logger.error(f"Error generating auth log: {str(e)}")
#                 continue
        
#         logger.info(f"Successfully generated {len(auth_logs_data)} authentication logs")
#         return auth_logs_data

#     def generate_all_data(self, customer_count=10):
#         """Generate all types of data and return as dictionary"""
#         logger.info("Starting data generation process...")
#         logger.info(f"Generating data for {customer_count} customers...")
#         try:
#             # Generate data in sequence
#             customers_data = self.generate_customers(customer_count)
#             if not customers_data:
#                 raise Exception("Failed to generate customers")
                
#             devices_data = self.generate_devices(customers_data)

#             accounts_data = self.generate_accounts(customers_data)

#             transactions_data = []
#             if accounts_data:
#                 transactions_data = self.generate_transactions(accounts_data)

#             auth_logs_data = []
#             if customers_data and devices_data:
#                 auth_logs_data = self.generate_auth_logs(customers_data, devices_data)

#             # Return all generated data as a dictionary
#             generated_data = {
#                 'customers': customers_data,
#                 'devices': devices_data,
#                 'accounts': accounts_data,
#                 'transactions': transactions_data,
#                 'auth_logs': auth_logs_data
#             }
            
#             print(f'generated_data: {generated_data}')

#             logger.info("Data generation completed successfully")
#             logger.info(f"Generated: {len(customers_data)} customers, {len(devices_data)} devices, "
#                        f"{len(accounts_data)} accounts, {len(transactions_data)} transactions, "
#                        f"{len(auth_logs_data)} auth logs")
            
#             return generated_data
            
#         except Exception as e:
#             logger.error(f"Data generation failed: {str(e)}")
#             raise

# def main():
#     """Main function for standalone execution"""
#     try:
#         # Parse arguments
#         parser = argparse.ArgumentParser(description="Generate synthetic banking data")
#         parser.add_argument('--output_dir', required=True, help='Directory to save generated data')
#         args = parser.parse_args()

#         output_dir = args.output_dir
#         os.makedirs(output_dir, exist_ok=True)

#         # Get customer count from env
#         customer_count = int(os.getenv('CUSTOMER_COUNT', '10'))

#         generator = BankDataGenerator()
#         generated_data = generator.generate_all_data(customer_count)
        
#         if generated_data:

#             logger.info("Data generation process completed successfully")

#             logger.info(f'Generated data summary: {generated_data}')

#             # Save each component to separate JSON files
#             for key, records in generated_data.items():
#                 out_file = os.path.join(output_dir, f"{key}.json")
#                 with open(out_file, 'w') as f:
#                     records = convert_datetimes(records)
#                     json.dump(records, f, indent=2)
#                 logger.info(f"Saved {len(records)} records to {out_file}")
#                 print(f"Saved {len(records)} records to {out_file}")

#             print(f"Generated data summary:")
#             print(f"- Customers: {len(generated_data['customers'])}")
#             print(f"- Devices: {len(generated_data['devices'])}")
#             print(f"- Accounts: {len(generated_data['accounts'])}")
#             print(f"- Transactions: {len(generated_data['transactions'])}")
#             print(f"- Auth Logs: {len(generated_data['auth_logs'])}")

#             # Optional print summary
#             print(f"Generated data summary: {json.dumps(generated_data, indent=2)}")
#             for key in generated_data:
#                 print(f"- {key.capitalize()}: {len(generated_data[key])}")

#             # LAST LINE: Only print output dir (for Airflow to capture it)
#             print(output_dir)
#         else:
#             logger.error("Data generation returned empty")
#             return 1

#     except Exception as e:
#         logger.error(f"Fatal error in data generation: {str(e)}")
#         return 1


# if __name__ == "__main__":
#     main()

