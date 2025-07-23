#!/usr/bin/env python3
import json
import argparse
import os
import logging
from datetime import datetime

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

class BankingMonitor:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.data = {}
        self.alerts = []
        
    def load_data(self):
        for table in ['customers', 'accounts', 'transactions', 'auth_logs', 'devices']:
            path = os.path.join(self.data_dir, f'{table}.json')
            try:
                with open(path, 'r') as f:
                    self.data[table] = json.load(f)
            except FileNotFoundError:
                # logger.warning(f"{table}.json not found")
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
            if amount > 10000000 and AuthMethod not in ['BIOMETRIC', 'OTP']:
                violations += 1
                print(f"High value transaction without strong auth: {amount} VND")
                self.add_alert(CustomerID, 'HIGH_VALUE_NO_STRONG_AUTH', 'HIGH',
                             f'Transaction {amount:,.0f} VND without strong auth', 
                             txn.get('TransactionID'))
            
            # Unverified device
            if not device_map.get(txn.get('DeviceID'), False):
                violations += 1
                self.add_alert(CustomerID, 'UNVERIFIED_DEVICE', 'MEDIUM',
                             'Transaction from unverified device',
                             txn.get('TransactionID'))
            
            # Daily limit tracking
            txn_date = timestamp[:10] if timestamp else ''
            if txn_date == today and not txn.get('risk_flag', False):
                daily_totals[CustomerID] = daily_totals.get(CustomerID, 0) + amount
                if AuthMethod in ['BIOMETRIC', 'OTP']:
                    strong_auths.add(CustomerID)

        # # Check daily limits
        # for CustomerID, total in daily_totals.items():
        #     if total > 20000000 and CustomerID not in strong_auths:
        #         violations += 1
        #         self.add_alert(CustomerID, 'DAILY_LIMIT_NO_STRONG_AUTH', 'HIGH',
        #                      f'Daily total {total:,.0f} VND without strong auth')
        
        # logger.info(f"Risk violations: {violations}")
        return violations

    def run_audit(self):
        # logger.info("Starting audit")
        self.load_data()
        
        risk_violations = self.check_risks()
        print(f'Risk violations: {risk_violations}')
        # Save alerts
        alerts_file = os.path.join(self.data_dir, 'risk_alerts.json')
        with open(alerts_file, 'w') as f:
            json.dump(self.alerts, f, indent=2)
        
        # logger.info(f"Audit complete - Risks: {risk_violations}, Alerts: {len(self.alerts)}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', required=True, help='JSON files directory')
    args = parser.parse_args()
    
    monitor = BankingMonitor(args.dir)
    monitor.run_audit()
    exit(0)

if __name__ == "__main__":
    main()