#!/usr/bin/env python3
import json
import argparse
import os
import logging
import psycopg2
from datetime import datetime


class BankingMonitor:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.data = {}
        self.alerts = []
    
    def get_db_connection(self):
        try:
            return psycopg2.connect(
                host=os.getenv('DB_HOST', 'postgres_data'), port=os.getenv('DB_PORT', '5432'),
                database=os.getenv('DB_NAME', 'mydata'), user=os.getenv('DB_USER', 'user'),
                password=os.getenv('DB_PASSWORD', 'userpass'))
        except Exception as e:
            return None
        
    def load_data(self):
        for table in ['customers', 'accounts', 'transactions', 'auth_logs', 'devices']:
            path = os.path.join(self.data_dir, f'{table}.json')
            try:
                with open(path, 'r') as f:
                    self.data[table] = json.load(f)
            except FileNotFoundError:
                self.data[table] = []

    def add_alert(self, CustomerID, alert_type, level, description, TransactionID=None):
        self.alerts.append({
            'CustomerID': CustomerID,
            'TransactionID': TransactionID,
            'alert_type': alert_type,
            'alert_level': level,
            'description': description,
            'timestamp': datetime.now().isoformat()
        })

    def check_risks(self):
        violations = 0
        # Create mappings
        account_map = {a.get('AccountID'): a.get('CustomerID') 
                      for a in self.data.get('accounts', [])}
        
        device_map = {d.get('DeviceID'): d.get('is_verified', False) 
                     for d in self.data.get('devices', [])}
        
        # Create auth method mapping by customer and timestamp
        AuthMethods = {}
        for auth in self.data.get('auth_logs', []):
            if auth.get('auth_status') == 'SUCCESS':
                CustomerID = auth.get('CustomerID')
                timestamp = auth.get('timestamp')
                method = auth.get('AuthMethod')
                if CustomerID and timestamp and method:
                    AuthMethods[f"{CustomerID}_{timestamp}"] = method
        
        # Daily totals and strong auth tracking
        daily_totals = {}
        strong_auths = set()
        today = datetime.now().date().isoformat()
        conn = self.get_db_connection()
        if not conn:
            # logger.error("Failed to connect to database")
            return violations
        cursor = conn.cursor()

        for txn in self.data.get('transactions', []):
            CustomerID = account_map.get(txn.get('FromAccountID'))
            if not CustomerID:
                continue
                
            amount = float(txn.get('amount', 0))
            timestamp = txn.get('timestamp', '')
            
            # Get auth method from auth_logs
            auth_key = f"{CustomerID}_{timestamp}"
            AuthMethod = AuthMethods.get(auth_key, 'PASSWORD')
            # High value without strong auth
            if amount > 10000000 and AuthMethod not in ['BIOMETRIC']:
                violations += 1
                self.add_alert(CustomerID, 'HIGH_VALUE_NO_STRONG_AUTH', 'HIGH',
                             f'Transaction {amount:,.0f} VND without strong auth', 
                             txn.get('TransactionID'))
            
            # Unverified device
            try:
                cursor.execute(
                    "SELECT IsVerified FROM Device WHERE DeviceID = %s", (txn.get('DeviceID'),))
                is_verified = cursor.fetchone()[0] or False
            except Exception as e:
                is_verified = False
                
            if not device_map.get(txn.get('DeviceID'), False) and not is_verified:
                violations += 1
                self.add_alert(CustomerID, 'UNVERIFIED_DEVICE', 'MEDIUM',
                             'Transaction from unverified device',
                             txn.get('TransactionID'))
            
            try: 
                cursor.execute(
                    "SELECT SUM(Amount) FROM Transaction WHERE FromAccountID = %s AND DATE(Timestamp) = %s",
                    (txn.get('FromAccountID'), today))
                total = cursor.fetchone()[0] or 0
            except Exception as e:
                total = 0

            txn_date = timestamp[:10] if timestamp else ''
            if txn_date == today and not txn.get('risk_flag', False):
                daily_totals[CustomerID] = daily_totals.get(CustomerID, 0) + amount
                total = daily_totals[CustomerID] + total
                if AuthMethod in ['BIOMETRIC', 'OTP']:
                    strong_auths.add(CustomerID)
            if total > 20000000 and CustomerID not in strong_auths:
                violations += 1
                self.add_alert(CustomerID, 'DAILY_LIMIT_NO_STRONG_AUTH', 'HIGH',
                             f'Daily total {total:,.0f} VND without strong auth',
                             txn.get('TransactionID'))

        return violations

    def run_audit(self):
        self.load_data()
        
        risk_violations = self.check_risks()
        print(f'Risk violations: {risk_violations}')
        # Save alerts
        alerts_file = os.path.join(self.data_dir, 'risk_alerts.json')
        with open(alerts_file, 'w') as f:
            json.dump(self.alerts, f, indent=2)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', required=True, help='JSON files directory')
    args = parser.parse_args()
    
    monitor = BankingMonitor(args.dir)
    monitor.run_audit()
    exit(0)

if __name__ == "__main__":
    main()