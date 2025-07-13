import random
import string
from datetime import datetime, timedelta
import psycopg2
from faker import Faker

fake = Faker()

# Database connection params
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="mydata",
    user="user",
    password="userpass"
)
cursor = conn.cursor()

# Utility functions
def random_password():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=12))

def random_currency():
    return random.choice(['USD', 'VND', 'EUR'])

def random_status():
    return random.choice(['ACTIVE', 'FROZEN', 'CLOSED'])

def random_account_type():
    return random.choice(['CHECKING', 'SAVINGS', 'BUSINESS'])

def insert_customers(n=10):
    customers = []
    for _ in range(n):
        national_id = fake.unique.ssn()
        name = fake.name()
        address = fake.address().replace("\n", ", ")
        contact = fake.phone_number()
        username = fake.unique.user_name()
        password_hash = random_password()
        cursor.execute("""
            INSERT INTO Customer (NationalID, Name, Address, Contact, Username, PasswordHash)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING CustomerID
        """, (national_id, name, address, contact, username, password_hash))
        customers.append(cursor.fetchone()[0])
    return customers

def insert_devices(customers):
    devices = []
    for cust_id in customers:
        for _ in range(random.randint(1, 2)):
            dtype = random.choice(['Phone', 'Tablet', 'Laptop'])
            dinfo = fake.user_agent()
            verified = random.choice([True, False])
            last_used = fake.date_time_between(start_date='-30d', end_date='now')
            cursor.execute("""
                INSERT INTO Device (CustomerID, DeviceType, DeviceInfo, IsVerified, LastUsed)
                VALUES (%s, %s, %s, %s, %s) RETURNING DeviceID
            """, (cust_id, dtype, dinfo, verified, last_used))
            devices.append(cursor.fetchone()[0])
    return devices

def insert_accounts(customers):
    accounts = []
    for cust_id in customers:
        for _ in range(random.randint(1, 3)):
            acc_type = random_account_type()
            balance = round(random.uniform(1000, 1000000), 2)
            currency = random_currency()
            status = random_status()
            cursor.execute("""
                INSERT INTO Account (CustomerID, AccountType, Balance, Currency, Status)
                VALUES (%s, %s, %s, %s, %s) RETURNING AccountID
            """, (cust_id, acc_type, balance, currency, status))
            accounts.append(cursor.fetchone()[0])
    return accounts

def insert_transactions(accounts):
    for _ in range(30):
        from_acc, to_acc = random.sample(accounts, 2)
        amount = round(random.uniform(100, 50000), 2)
        txn_type = random.choice(['TRANSFER', 'PAYMENT'])
        ts = fake.date_time_between(start_date='-10d', end_date='now')
        risk = amount > 20000
        cursor.execute("""
            INSERT INTO Transaction (FromAccountID, ToAccountID, TxnType, Amount, Timestamp, RiskFlag)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (from_acc, to_acc, txn_type, amount, ts, risk))

def insert_auth_logs(customers, devices):
    for _ in range(20):
        cust_id = random.choice(customers)
        device_id = random.choice(devices)
        method = random.choice(['OTP', 'Biometric', 'Password'])
        status = random.choice(['SUCCESS', 'FAIL'])
        ts = fake.date_time_between(start_date='-7d', end_date='now')
        cursor.execute("""
            INSERT INTO AuthenticationLog (CustomerID, DeviceID, AuthMethod, AuthStatus, Timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """, (cust_id, device_id, method, status, ts))

# Run all inserts
customers = insert_customers(10)
devices = insert_devices(customers)
accounts = insert_accounts(customers)
insert_transactions(accounts)
insert_auth_logs(customers, devices)

conn.commit()
cursor.close()
conn.close()

print("✅ Sample data inserted successfully.")
