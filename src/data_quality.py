import sys
import logging
import json
import argparse
import os
import psycopg2
import re
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self, data=None):
        self.data = data or {}
        self.quality_issues = []
        self.failed_records = {}
        
    def get_db_connection(self):
        try:
            return psycopg2.connect(
                host=os.getenv('DB_HOST', 'postgres_data'), port=os.getenv('DB_PORT', '5432'),
                database=os.getenv('DB_NAME', 'mydata'), user=os.getenv('DB_USER', 'user'),
                password=os.getenv('DB_PASSWORD', 'userpass'))
        except Exception as e:
            logger.warning(f"Database connection failed: {str(e)}")
            return None
    
    def check_all_quality(self):
        logger.info("Starting data quality checks...")
        if not self.data:
            logger.error("No data provided")
            return False
        

        self.quality_issues = []
        self.failed_records = {}

        tables_to_check = [
            ('customers', 'Customer', ['CustomerID', 'NationalID']),
            ('devices', 'Device', ['DeviceID', 'CustomerID']),
            ('accounts', 'Account', ['AccountID', 'CustomerID']),
            ('transactions', 'Transaction', ['TransactionID', 'FromAccountID', 'ToAccountID', 'DeviceID']),
            ('auth_logs', 'AuthenticationLog', ['AuthID', 'CustomerID', 'DeviceID']),
        ]

        special_format_fields = {
            'NationalID': r'^\d{12}$'
        }

        seen_values = {}
        field_to_entity = {}
        
        fail_special_format = []
        pending_fks = {}

        for entity_key, db_table, fields in tables_to_check:
            if entity_key not in self.data:
                continue

            self.failed_records[entity_key] = []
            records = self.data[entity_key]

            for i, record in enumerate(records):
                record_failed = False

                for field in fields:
                    field_to_entity[field] = db_table
                    value = record.get(field)
                    if not value or (isinstance(value, str) and not value.strip()):
                        self.quality_issues.append(f"NULL/missing {field} in {entity_key}")
                        self.failed_records[entity_key].append(record)
                        record_failed = True
                        break

                    if isinstance(value, str):
                        value = value.strip()

                    record[field] = value

                    # Kiểm tra định dạng đặc biệt
                    if field in special_format_fields:
                        if not re.match(special_format_fields[field], value):
                            self.quality_issues.append(f"Invalid {field} format: {value}")
                            self.failed_records[entity_key].append(record)
                            record_failed = True
                            fail_special_format.append(record)
                            break

                if record_failed:
                    continue

                if entity_key == 'customers':
                    customer_id = record.get('CustomerID')
                    national_id = record.get('NationalID')

                    if 'CustomerID' not in seen_values:
                        seen_values['CustomerID'] = {}
                        field_to_entity['CustomerID'] = entity_key
                    if 'NationalID' not in seen_values:
                        seen_values['NationalID'] = {}
                    duplicate_found = False

                    if customer_id is not None and customer_id in seen_values['CustomerID']:
                        self.quality_issues.append(f"Duplicate CustomerID: {customer_id}")
                        duplicate_found = True
                    elif national_id is not None and national_id in seen_values['NationalID']:
                        self.quality_issues.append(f"Duplicate NationalID: {national_id}")
                        duplicate_found = True

                    if duplicate_found:
                        self.failed_records[entity_key].append(record)
                        continue 

                    seen_values['CustomerID'][customer_id] = i
                    seen_values['NationalID'][national_id] = i

                else:
                    # Only check primary key for duplicates
                    id_field = fields[0]
                    id_value = record.get(id_field)
                    if id_value is not None:
                        if id_field not in seen_values:
                            seen_values[id_field] = {}
                            field_to_entity[id_field] = db_table

                        if id_value in seen_values[id_field]:
                            self.quality_issues.append(f"Duplicate {id_field}: {id_value}")
                            self.failed_records[entity_key].append(record)
                            continue  # skip to next record

                # === Foreign Key Check ===
                
                if entity_key != 'customers':
                    check = True
                    fk_fields = fields[1:]  # Skip the first field which is the primary key
                    for fk_field in fk_fields:
                        actual_fk = 'AccountID' if fk_field in ['FromAccountID', 'ToAccountID'] else fk_field
                        fk_value = record.get(fk_field)

                        if actual_fk in seen_values:
                            if fk_value not in seen_values[actual_fk]:
                                if actual_fk not in pending_fks:
                                    pending_fks[actual_fk] = {}

                                if fk_value not in pending_fks[actual_fk]:
                                    pending_fks[actual_fk][fk_value] = {}

                                if entity_key not in pending_fks[actual_fk][fk_value]:
                                    pending_fks[actual_fk][fk_value][entity_key] = i   

                                self.quality_issues.append(
                                    f"Invalid foreign key {fk_field}: {fk_value} not found in {field_to_entity[actual_fk]}"
                                )
                                self.failed_records[entity_key].append(record)
                                check = False
                                break
                        else:
                            self.quality_issues.append(
                                f"Foreign key reference field {actual_fk} not yet loaded when processing {fk_field}"
                            )
                            if actual_fk not in pending_fks:
                                pending_fks[actual_fk] = {}

                            if fk_value not in pending_fks[actual_fk]:
                                pending_fks[actual_fk][fk_value] = {}

                            if entity_key not in pending_fks[actual_fk][fk_value]:
                                pending_fks[actual_fk][fk_value][entity_key] = i 
                            self.failed_records[entity_key].append(record)
                            check = False
                            break

                    if check:
                        seen_values[id_field][id_value] = i

        conn = self.get_db_connection()
        if conn:
            try:
                cursor = conn.cursor()
                for entity_key, db_table, fields in tables_to_check:
                    id_field = fields[0]
                    if entity_key == 'customers':
                        if not seen_values.get('CustomerID') and not seen_values.get('NationalID'):
                            continue
                        id_customer = list(seen_values['CustomerID'].keys())
                        id_NationalID = list(seen_values['NationalID'].keys())
                        placeholders_CustomerID = ','.join(['%s'] * len(id_customer))
                        placeholders_NationalID = ','.join(['%s'] * len(id_NationalID))
                        try:
                            cursor.execute(
                                f"SELECT CustomerID, NationalID FROM {db_table} WHERE CustomerID IN ({placeholders_CustomerID}) OR NationalID IN ({placeholders_NationalID})",
                                id_customer + id_NationalID
                            )
                            for row in cursor.fetchall():
                                customer_id = row[0]
                                national_id = row[1]
                                if customer_id in seen_values['CustomerID'] or national_id in seen_values['NationalID']:
                                    if customer_id in seen_values['CustomerID']:
                                        idx = seen_values['CustomerID'][customer_id]
                                    elif national_id in seen_values['NationalID']:
                                        idx = seen_values['NationalID'][national_id]
                                    record = self.data[entity_key][idx]
                                    if customer_id in seen_values['CustomerID']:
                                        self.quality_issues.append(f"CustomerID exists in database table {db_table}: {customer_id}")
                                    else :
                                        self.quality_issues.append(f"NationalID exists in database table {db_table}: {national_id}")
                                    if record not in self.failed_records[entity_key]:
                                        self.failed_records[entity_key].append(record)
                        except Exception as e:
                            logger.warning(f"DB check failed for {id_field} in {db_table}: {str(e)}")
                            continue
                        
                        continue
                    else:
                        if not seen_values.get(id_field):
                            continue
                        ids_to_check = list(seen_values[id_field].keys())
                        placeholders = ','.join(['%s'] * len(ids_to_check))
                        
                        try:
                            cursor.execute(
                                f"SELECT {id_field} FROM {db_table} WHERE {id_field} IN ({placeholders})",
                                ids_to_check
                            )
                            existing_ids = set(row[0] for row in cursor.fetchall())
                            
                            for existing_id in existing_ids:
                                if existing_id in seen_values[id_field]:
                                    idx = seen_values[id_field][existing_id]
                                    record = self.data[entity_key][idx]
                                    self.quality_issues.append(f"{id_field} exists in database table {db_table}: {existing_id}")
                                    if record not in self.failed_records[entity_key]:
                                        self.failed_records[entity_key].append(record)
                                        
                        except Exception as e:
                            logger.warning(f"DB check failed for {id_field} in {db_table}: {str(e)}")
                            continue

                for actual_fk, entities in pending_fks.items():
                    # tập tất cả giá trị FK cần check
                    fk_values = list(entities.keys())
                    if not fk_values:
                        continue

                    placeholders = ','.join(['%s'] * len(fk_values))
                    table_name = field_to_entity[actual_fk]
                    try:
                        cursor.execute(
                            f"SELECT {actual_fk} FROM {table_name} WHERE {actual_fk} IN ({placeholders})",
                            fk_values
                        )
                        existing = set(row[0] for row in cursor.fetchall())
                        missing = set(fk_values) - existing
                    except Exception as e:
                        logger.warning(f"DB FK check batch failed for {actual_fk}: {e}")
                        missing = set()


                    for fk_val in missing:
                        if fk_val not in pending_fks[actual_fk]:
                            continue
                        for entity_key in pending_fks[actual_fk][fk_val]:
                            idx = pending_fks[actual_fk][fk_val][entity_key]
                            if entity_key not in self.failed_records:
                                self.failed_records[entity_key] = []
                            if self.data[entity_key][idx] not in self.failed_records[entity_key]:
                                self.failed_records[entity_key].append(self.data[entity_key][idx])

                            self.quality_issues.append(
                                f"Foreign key {actual_fk} not found in DB: {table_name}"
                            )

                cursor.close()
                conn.close()
            except Exception as e:
                logger.warning(f"Database connection or query failed: {str(e)}")
                conn.close()

        total_issues = len(self.quality_issues)
        if total_issues > 0:
            logger.error(f"Found {total_issues} data quality issues")
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
    data = {}
    for filename in os.listdir(input_dir):
        if filename.endswith(".json"):
            file_path = os.path.join(input_dir, filename)
            with open(file_path, 'r') as f:
                try:
                    data[filename.replace(".json", "")] = json.load(f)
                    logger.info(f"Loaded {len(data[filename.replace('.json', '')])} {filename.replace('.json', '')}")
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to load {filename}: {e}")
                    data[filename.replace(".json", "")] = []
            try:
                os.remove(file_path)
                logger.info(f"Deleted {filename}")
            except Exception as e:
                logger.warning(f"Could not delete {file_path}: {e}")
    return data

def convert_datetime(obj):
    if isinstance(obj, list):
        return [convert_datetime(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_datetime(value) for key, value in obj.items()}
    elif isinstance(obj, datetime):
        return obj.isoformat()
    return obj

def save_clean_data(clean_data, output_dir, failed_data=None):
    os.makedirs(output_dir, exist_ok=True)
    for table_name, records in clean_data.items():
        if records:
            with open(os.path.join(output_dir, f"{table_name}.json"), 'w') as f:
                json.dump(convert_datetime(records), f, indent=2)
            logger.info(f"Saved {len(records)} clean {table_name}")
    
    if failed_data and any(failed_data.values()):
        with open(os.path.join(output_dir, "failed_records.json"), 'w') as f:
            json.dump(failed_data, f, indent=2)
        logger.info("Saved failed records log")
    
def main():
    parser = argparse.ArgumentParser(description="Data quality checker")
    parser.add_argument('--input_dir', required=True, help='Input directory')
    args = parser.parse_args()

    if not os.path.exists(args.input_dir):
        logger.error(f"Directory not found: {args.input_dir}")
        sys.exit(1)

    # Load data and run checks
    data_dict = load_and_cleanup_files(args.input_dir)
    logger.info("Data loaded, original files deleted")
    
    checker = DataQualityChecker(data_dict)
    success = checker.check_all_quality()
    
    # Save results
    clean_data = checker.get_clean_data()
    failed_data = {"failed_records": checker.failed_records, "issues": checker.quality_issues}
    save_clean_data(clean_data, args.input_dir, failed_data)
    
    if len(checker.quality_issues) > 0:
        print(f"Quality check completed - {len(checker.quality_issues)} issues found")
        print("Clean data saved - check failed_records.json for details")
    else:
        print("Quality check passed - all data saved")
    
    sys.exit(0)

if __name__ == "__main__":
    main()









