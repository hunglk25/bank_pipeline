import sys
import logging
from datetime import datetime
import json
import argparse
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self, data=None):
        self.data = data or {}
        self.quality_issues = []
        self.failed_records = {}
        
    def check_all_quality(self):
        """Run all data quality checks and track failed records"""
        logger.info("Starting data quality checks...")
        
        if not self.data:
            logger.error("No data provided")
            return False
        
        # Clear previous issues
        self.quality_issues = []
        self.failed_records = {}
        
        # Check NULL values
        critical_fields = {
            'customers': ['NationalID', 'Name', 'Username'],
            'accounts': ['CustomerID', 'Balance'],
            'transactions': ['Amount', 'FromAccountID', 'ToAccountID']
        }
        
        for data_type, fields in critical_fields.items():
            if data_type not in self.data:
                continue
                
            self.failed_records[data_type] = []
            
            for item in self.data[data_type]:
                has_issue = False
                
                # Check NULL values
                for field in fields:
                    if field not in item or item[field] is None or item[field] == '':
                        issue = f"NULL value in {data_type}.{field}"
                        self.quality_issues.append(issue)
                        logger.warning(issue)
                        has_issue = True
                
                # Check business rules for accounts
                if data_type == 'accounts' and item.get('Balance', 0) < 0:
                    issue = f"Negative balance in account {item.get('AccountID')}"
                    self.quality_issues.append(issue)
                    logger.warning(issue)
                    has_issue = True
                
                # Check business rules for transactions
                if data_type == 'transactions' and item.get('Amount', 0) <= 0:
                    issue = f"Invalid amount in transaction {item.get('TransactionID')}"
                    self.quality_issues.append(issue)
                    logger.warning(issue)
                    has_issue = True
                
                if has_issue:
                    self.failed_records[data_type].append(item)
        
        # Check duplicates in customers
        if 'customers' in self.data:
            if 'customers' not in self.failed_records:
                self.failed_records['customers'] = []
                
            seen_values = {}
            for i, customer in enumerate(self.data['customers']):
                for field in ['NationalID', 'Username']:
                    if field in customer and customer[field]:
                        value = customer[field]
                        if value in seen_values:
                            issue = f"Duplicate {field}: {value}"
                            self.quality_issues.append(issue)
                            logger.warning(issue)
                            # Mark both records as failed
                            original_idx = seen_values[value]
                            if self.data['customers'][original_idx] not in self.failed_records['customers']:
                                self.failed_records['customers'].append(self.data['customers'][original_idx])
                            if customer not in self.failed_records['customers']:
                                self.failed_records['customers'].append(customer)
                        else:
                            seen_values[value] = i
        
        total_issues = len(self.quality_issues)
        if total_issues > 0:
            logger.error(f"Found {total_issues} quality issues")
            return False
        else:
            logger.info("All data quality checks passed")
            return True
    
    def get_clean_data(self):
        """Return clean data (remove failed records)"""
        clean_data = {}
        
        for data_type, records in self.data.items():
            failed_records_for_type = self.failed_records.get(data_type, [])
            clean_records = [record for record in records if record not in failed_records_for_type]
            clean_data[data_type] = clean_records
            logger.info(f"Clean {data_type}: {len(clean_records)}/{len(records)} records")
        
        return clean_data

def load_and_cleanup_files(input_dir):
    """Load JSON files and delete them"""
    data = {}
    
    for filename in os.listdir(input_dir):
        if filename.endswith(".json"):
            table_name = filename.replace(".json", "")
            file_path = os.path.join(input_dir, filename)
            
            with open(file_path, 'r') as f:
                try:
                    data[table_name] = json.load(f)
                    logger.info(f"Loaded {len(data[table_name])} {table_name}")
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to load {filename}: {e}")
                    data[table_name] = []
            
            # Delete file after loading
            try:
                os.remove(file_path)
                logger.info(f"Deleted {filename}")
            except Exception as e:
                logger.warning(f"Could not delete {file_path}: {e}")
    
    return data

def save_clean_data(clean_data, output_dir, failed_data=None):
    """Save clean data back to JSON files"""
    os.makedirs(output_dir, exist_ok=True)
    
    def convert_datetime(obj):
        if isinstance(obj, list):
            return [convert_datetime(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: convert_datetime(value) for key, value in obj.items()}
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj
    
    # Save clean data
    for table_name, records in clean_data.items():
        if records:
            output_file = os.path.join(output_dir, f"{table_name}.json")
            with open(output_file, 'w') as f:
                clean_records = convert_datetime(records)
                json.dump(clean_records, f, indent=2)
            logger.info(f"Saved {len(records)} clean {table_name}")
    
    # Save failed data log
    if failed_data and any(failed_data.values()):
        failed_log = os.path.join(output_dir, "failed_records.json")
        with open(failed_log, 'w') as f:
            json.dump(failed_data, f, indent=2)
        logger.info(f"Saved failed records log")

def main():
    parser = argparse.ArgumentParser(description="Data quality checker")
    parser.add_argument('--input_dir', required=True, help='Input directory')
    args = parser.parse_args()

    input_dir = args.input_dir
    
    if not os.path.exists(input_dir):
        logger.error(f"Directory not found: {input_dir}")
        sys.exit(1)

    # Load data and cleanup files
    data_dict = load_and_cleanup_files(input_dir)
    logger.info("Data loaded, original files deleted")
    
    # Run quality checks
    checker = DataQualityChecker(data_dict)
    success = checker.check_all_quality()
    
    # Get clean data and save
    clean_data = checker.get_clean_data()
    failed_data = {
        "failed_records": checker.failed_records,
        "issues": checker.quality_issues
    }
    
    save_clean_data(clean_data, input_dir, failed_data)
    
    if len(checker.quality_issues) > 0:
        logger.info(f"Completed with {len(checker.quality_issues)} issues resolved")
        print(f"Quality check completed - {len(checker.quality_issues)} issues found")
        print("Clean data saved - check failed_records.json for details")
    else:
        logger.info("All data passed quality checks")
        print("Quality check passed - all data saved")
    
    sys.exit(0)

if __name__ == "__main__":
    main()










'''
import sys
import logging
from datetime import datetime
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self, data=None):
        """
        Initialize DataQualityChecker with data dictionary
        Expected data format:
        {
            'customers': [...],
            'devices': [...],
            'accounts': [...],
            'transactions': [...],
            'auth_logs': [...]
        }
        """
        self.data = data or {}
        self.quality_issues = []
        
    def check_null_values(self):
        """Check for NULL/None values in critical fields"""
        critical_fields = {
            'customers': ['NationalID', 'Name', 'Username'],
            'accounts': ['CustomerID', 'Balance'],
            'transactions': ['Amount', 'FromAccountID', 'ToAccountID']
        }
        
        for data_type, fields in critical_fields.items():
            if data_type not in self.data:
                continue
                
            for item in self.data[data_type]:
                for field in fields:
                    if field not in item or item[field] is None or item[field] == '':
                        issue = f"Found NULL/empty value in {data_type}.{field} for item {item.get('CustomerID', item.get('AccountID', item.get('TransactionID', 'Unknown')))}"
                        self.quality_issues.append(issue)
                        logger.warning(issue)
    
    def check_duplicate_values(self):
        """Check for duplicate values in unique fields"""
        unique_checks = {
            'customers': ['NationalID', 'Username']
        }
        
        for data_type, fields in unique_checks.items():
            if data_type not in self.data:
                continue
                
            for field in fields:
                values_seen = set()
                for item in self.data[data_type]:
                    if field in item and item[field] is not None:
                        value = item[field]
                        if value in values_seen:
                            issue = f"Found duplicate value '{value}' in {data_type}.{field}"
                            self.quality_issues.append(issue)
                            logger.warning(issue)
                        else:
                            values_seen.add(value)
    
    def check_referential_integrity(self):
        """Check foreign key constraints"""
        # Create lookup dictionaries for faster checking
        customer_ids = {item['CustomerID'] for item in self.data.get('customers', [])}
        account_ids = {item['AccountID'] for item in self.data.get('accounts', [])}
        device_ids = {item['DeviceID'] for item in self.data.get('devices', [])}
        
        # Check Device -> Customer relationship
        for device in self.data.get('devices', []):
            if device.get('CustomerID') and device['CustomerID'] not in customer_ids:
                issue = f"Device {device.get('DeviceID')} references non-existent CustomerID {device['CustomerID']}"
                self.quality_issues.append(issue)
                logger.warning(issue)
        
        # Check Account -> Customer relationship  
        for account in self.data.get('accounts', []):
            if account.get('CustomerID') and account['CustomerID'] not in customer_ids:
                issue = f"Account {account.get('AccountID')} references non-existent CustomerID {account['CustomerID']}"
                self.quality_issues.append(issue)
                logger.warning(issue)
        
        # Check Transaction -> Account relationships
        for transaction in self.data.get('transactions', []):
            if transaction.get('FromAccountID') and transaction['FromAccountID'] not in account_ids:
                issue = f"Transaction {transaction.get('TransactionID')} references non-existent FromAccountID {transaction['FromAccountID']}"
                self.quality_issues.append(issue)
                logger.warning(issue)
                
            if transaction.get('ToAccountID') and transaction['ToAccountID'] not in account_ids:
                issue = f"Transaction {transaction.get('TransactionID')} references non-existent ToAccountID {transaction['ToAccountID']}"
                self.quality_issues.append(issue)
                logger.warning(issue)
        
        # Check AuthenticationLog relationships
        for auth_log in self.data.get('auth_logs', []):
            if auth_log.get('CustomerID') and auth_log['CustomerID'] not in customer_ids:
                issue = f"AuthLog {auth_log.get('AuthLogID')} references non-existent CustomerID {auth_log['CustomerID']}"
                self.quality_issues.append(issue)
                logger.warning(issue)
                
            if auth_log.get('DeviceID') and auth_log['DeviceID'] not in device_ids:
                issue = f"AuthLog {auth_log.get('AuthLogID')} references non-existent DeviceID {auth_log['DeviceID']}"
                self.quality_issues.append(issue)
                logger.warning(issue)
    
    def check_business_rules(self):
        """Check business-specific rules"""
        
        # Check for negative balances
        negative_balance_count = 0
        for account in self.data.get('accounts', []):
            if account.get('Balance') is not None and account['Balance'] < 0:
                negative_balance_count += 1
        
        if negative_balance_count > 0:
            issue = f"Found {negative_balance_count} accounts with negative balance"
            self.quality_issues.append(issue)
            logger.warning(issue)
        
        # Check for transactions with zero or negative amount
        zero_amount_count = 0
        for transaction in self.data.get('transactions', []):
            if transaction.get('Amount') is not None and transaction['Amount'] <= 0:
                zero_amount_count += 1
        
        if zero_amount_count > 0:
            issue = f"Found {zero_amount_count} transactions with zero or negative amount"
            self.quality_issues.append(issue)
            logger.warning(issue)
        
        # Check for future dates in transactions
        future_txn_count = 0
        now = datetime.now()
        for transaction in self.data.get('transactions', []):
            if transaction.get('Timestamp'):
                # Handle both string and datetime objects
                timestamp = transaction['Timestamp']
                if isinstance(timestamp, str):
                    try:
                        timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    except:
                        continue
                
                if timestamp > now:
                    future_txn_count += 1
        
        if future_txn_count > 0:
            issue = f"Found {future_txn_count} transactions with future timestamps"
            self.quality_issues.append(issue)
            logger.warning(issue)
    
    def get_quality_report(self):
        """Return quality issues as a structured report"""
        return {
            'timestamp': datetime.now().isoformat(),
            'total_issues': len(self.quality_issues),
            'issues': self.quality_issues,
            'status': 'PASSED' if len(self.quality_issues) == 0 else 'FAILED',
            'data_summary': {
                'customers': len(self.data.get('customers', [])),
                'devices': len(self.data.get('devices', [])),
                'accounts': len(self.data.get('accounts', [])),
                'transactions': len(self.data.get('transactions', [])),
                'auth_logs': len(self.data.get('auth_logs', []))
            }
        }
    
    def run_all_checks(self):
        """Run all data quality checks"""
        logger.info("Starting data quality checks...")
        
        if not self.data:
            logger.error("No data provided for quality checks")
            return False
        
        try:
            # Clear any previous issues
            self.quality_issues = []
            
            self.check_null_values()
            self.check_duplicate_values()
            self.check_referential_integrity()
            self.check_business_rules()
            
            if self.quality_issues:
                logger.error(f"Data quality check FAILED with {len(self.quality_issues)} issues")
                return False
            else:
                logger.info("Data quality check PASSED - no issues found")
                return True
                
        except Exception as e:
            logger.error(f"Error during data quality checks: {str(e)}")
            return False


import argparse
import json
import os
import sys
from datetime import datetime

def load_data_from_dir(input_dir):
    """
    Load all JSON files in a given directory into a dictionary.
    Assumes each file represents a table (e.g., customers.json → 'customers')
    """
    data = {}
    for filename in os.listdir(input_dir):
        if filename.endswith(".json"):
            table_name = filename.replace(".json", "")
            file_path = os.path.join(input_dir, filename)
            with open(file_path, 'r') as f:
                try:
                    data[table_name] = json.load(f)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to load {filename}: {e}")
                    data[table_name] = []
    return data


def check_data_quality(data_dict):
    """
    Run quality checks on the loaded data dictionary
    """
    logger.info("Running data quality checks on generated data...")
    
    if not data_dict:
        logger.error("No data provided for quality checks")
        return {"status": "FAILED", "total_issues": 1, "issues": ["No data loaded"], "data_summary": {}}
    
    checker = DataQualityChecker(data_dict)
    success = checker.run_all_checks()
    report = checker.get_quality_report()
    
    if success:
        logger.info("✅ Data quality checks PASSED")
        logger.info(f"Data summary: {report['data_summary']}")
        return report
    else:
        logger.error("❌ Data quality checks FAILED")
        logger.error(f"Found {report['total_issues']} issues:")
        for issue in report['issues']:
            logger.error(f"  - {issue}")
        return report

import shutil
def main():
    parser = argparse.ArgumentParser(description="Run data quality checks on generated data")
    parser.add_argument('--input_dir', required=True, help='Directory containing generated JSON data files')
    args = parser.parse_args()

    input_dir = args.input_dir

    logger.info(f"Loading data from directory: {input_dir}")
    
    if not os.path.exists(input_dir) or not os.path.isdir(input_dir):
        logger.error(f"Input directory does not exist: {input_dir}")
        sys.exit(1)

    data_dict = load_data_from_dir(input_dir)

    try:
        shutil.rmtree(input_dir)
        logger.info(f"Cleaned up input directory: {input_dir}")
    except Exception as e:
        logger.warning(f"Could not delete input directory: {e}")


    print("Data loaded successfully")
    print('--------------------------------------------------------------------------------------'* 3)
    print(f'Loaded data: {json.dumps(data_dict, indent=2)}')

    report = check_data_quality(data_dict)

    if report['status'] == 'PASSED':
        print("Quality check passed")
        sys.exit(0)
    else:
        print("Quality check failed")
        sys.exit(1)


if __name__ == "__main__":
    main()

'''














