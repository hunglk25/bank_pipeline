import json
import logging
import argparse
import os
from datetime import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RiskMonitor:
    def __init__(self, data):
        self.data = data
        self.violations = []
        self.violation_id = 1
        
    def log_violation(self, rule_code, description, risk_level, **kwargs):
        """Log a risk violation"""
        violation = {
            "ViolationID": self.violation_id,
            "Timestamp": datetime.now().isoformat(),
            "RuleCode": rule_code,
            "Description": description,
            "RiskLevel": risk_level,
            **kwargs
        }
        self.violations.append(violation)
        logger.warning(f"Violation {self.violation_id}: {rule_code} - {description}")
        self.violation_id += 1
        
    def check_all_risks(self):
        """Check all risk rules based on 5 main tables"""
        logger.info("Starting risk monitoring checks...")
        
        transactions = self.data.get('transactions', [])
        customers = self.data.get('customers', [])
        devices = self.data.get('devices', [])
        accounts = self.data.get('accounts', [])
        auth_logs = self.data.get('auth_logs', [])
        
        # Build lookup maps
        customer_map = {c['CustomerID']: c for c in customers}
        device_map = {d['DeviceID']: d for d in devices}
        account_map = {a['AccountID']: a for a in accounts}
        
        # Build auth map - match by CustomerID and recent timestamp
        auth_map = {}
        for auth in auth_logs:
            key = auth.get('CustomerID')
            if key and auth.get('AuthStatus') == 'SUCCESS':
                auth_map[key] = auth
        
        # Check transactions for risks
        daily_totals = defaultdict(float)
        
        for txn in transactions:
            txn_id = txn.get('TransactionID')
            amount = float(txn.get('Amount', 0))
            from_account_id = txn.get('FromAccountID')
            to_account_id = txn.get('ToAccountID')
            
            # Get customer from account
            from_account = account_map.get(from_account_id, {})
            customer_id = from_account.get('CustomerID')
            customer = customer_map.get(customer_id, {})
            
            # Rule 1: High value transactions (> 10M VND) need strong auth
            if amount > 10000000:
                auth = auth_map.get(customer_id, {})
                auth_method = auth.get('AuthMethod', '')
                if auth_method not in ['Biometric', 'OTP']:
                    self.log_violation(
                        "RULE_001",
                        f"High value transaction {amount:,.0f} VND requires strong authentication",
                        "HIGH",
                        TransactionID=txn_id,
                        CustomerID=customer_id,
                        Amount=amount,
                        AuthMethod=auth_method
                    )
            
            # Rule 2: Unverified devices for transactions
            if customer_id:
                # Find devices for this customer
                customer_devices = [d for d in devices if d.get('CustomerID') == customer_id]
                unverified_devices = [d for d in customer_devices if not d.get('IsVerified', False)]
                
                if unverified_devices and amount > 1000000:  # > 1M VND
                    self.log_violation(
                        "RULE_002", 
                        f"Transaction from unverified device requires verification",
                        "MEDIUM",
                        TransactionID=txn_id,
                        CustomerID=customer_id,
                        DeviceID=unverified_devices[0].get('DeviceID'),
                        Amount=amount
                    )
            
            # Rule 3: Account balance checks
            if from_account.get('Balance', 0) < amount:
                self.log_violation(
                    "RULE_003",
                    f"Insufficient balance for transaction",
                    "HIGH", 
                    TransactionID=txn_id,
                    CustomerID=customer_id,
                    FromAccountID=from_account_id,
                    Amount=amount,
                    Balance=from_account.get('Balance', 0)
                )
            
            # Accumulate daily totals
            if customer_id and txn.get('Timestamp'):
                date_key = txn['Timestamp'][:10]  # YYYY-MM-DD
                daily_totals[(customer_id, date_key)] += amount
        
        # Rule 4: Daily transaction limits (> 50M VND per day)
        for (customer_id, date), daily_total in daily_totals.items():
            if daily_total > 50000000:
                customer = customer_map.get(customer_id, {})
                self.log_violation(
                    "RULE_004",
                    f"Daily transaction limit exceeded: {daily_total:,.0f} VND",
                    "HIGH",
                    CustomerID=customer_id,
                    CustomerName=customer.get('Name', 'Unknown'),
                    Date=date,
                    DailyTotal=daily_total
                )
        
        # Rule 5: Failed authentication attempts
        failed_auths = defaultdict(int)
        for auth in auth_logs:
            if auth.get('AuthStatus') == 'FAIL':
                customer_id = auth.get('CustomerID')
                failed_auths[customer_id] += 1
        
        for customer_id, fail_count in failed_auths.items():
            if fail_count >= 3:  # 3+ failed attempts
                customer = customer_map.get(customer_id, {})
                self.log_violation(
                    "RULE_005",
                    f"Multiple failed authentication attempts: {fail_count}",
                    "MEDIUM",
                    CustomerID=customer_id,
                    CustomerName=customer.get('Name', 'Unknown'),
                    FailedAttempts=fail_count
                )
        
        logger.info(f"Risk monitoring completed. Found {len(self.violations)} violations")
        return len(self.violations) == 0

def load_json_files(input_dir):
    """Load JSON files from directory"""
    data = {}
    for filename in os.listdir(input_dir):
        if filename.endswith(".json"):
            file_path = os.path.join(input_dir, filename)
            with open(file_path, 'r') as f:
                try:
                    table_name = filename.replace(".json", "")
                    data[table_name] = json.load(f)
                    logger.info(f"Loaded {len(data[table_name])} {table_name}")
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to load {filename}: {e}")
    return data

def main():
    parser = argparse.ArgumentParser(description="Risk monitoring for banking data")
    parser.add_argument('--dir', required=True, help='Directory containing JSON files')
    args = parser.parse_args()
    
    if not os.path.exists(args.dir):
        logger.error(f"Directory not found: {args.dir}")
        return
    
    # Load data from JSON files
    data = load_json_files(args.dir)
    
    # Run risk monitoring
    monitor = RiskMonitor(data)
    passed = monitor.check_all_risks()
    
    # Save violations to JSON file
    if monitor.violations:
        violations_file = os.path.join(args.dir, "risk_violations.json")
        with open(violations_file, 'w') as f:
            json.dump(monitor.violations, f, indent=2)
        logger.info(f"Saved {len(monitor.violations)} violations to {violations_file}")
        
        # Also save summary
        summary = {
            "total_violations": len(monitor.violations),
            "by_risk_level": {},
            "by_rule": {},
            "generated_at": datetime.now().isoformat()
        }
        
        for violation in monitor.violations:
            risk_level = violation['RiskLevel']
            rule_code = violation['RuleCode']
            summary['by_risk_level'][risk_level] = summary['by_risk_level'].get(risk_level, 0) + 1
            summary['by_rule'][rule_code] = summary['by_rule'].get(rule_code, 0) + 1
        
        summary_file = os.path.join(args.dir, "risk_summary.json")
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Saved summary to {summary_file}")
    
    if passed:
        logger.info("No risk violations found")
        print("Risk monitoring passed - no violations detected")
    else:
        logger.warning(f"Found {len(monitor.violations)} risk violations")
        print(f"Risk monitoring completed - {len(monitor.violations)} violations found")

if __name__ == "__main__":
    main()