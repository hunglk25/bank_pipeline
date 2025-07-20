import psycopg2
import sys
import logging
from datetime import datetime
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self, host="postgres_data", port=5432, database="mydata", user="user", password="userpass"):
        self.conn_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        self.quality_issues = []
        
    def connect_db(self):
        """Create database connection"""
        try:
            conn = psycopg2.connect(**self.conn_params)
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise
    
    def check_null_values(self, conn):
        """Check for NULL values in critical fields"""
        critical_fields = [
            ('Customer', 'NationalID'),
            ('Customer', 'Name'),
            ('Customer', 'Username'),
            ('Account', 'CustomerID'),
            ('Account', 'Balance'),
            ('Transaction', 'Amount'),
            ('Transaction', 'FromAccountID'),
            ('Transaction', 'ToAccountID')
        ]
        
        cursor = conn.cursor()
        for table, field in critical_fields:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {field} IS NULL")
                null_count = cursor.fetchone()[0]
                if null_count > 0:
                    issue = f"Found {null_count} NULL values in {table}.{field}"
                    self.quality_issues.append(issue)
                    logger.warning(issue)
            except Exception as e:
                error = f"Error checking {table}.{field}: {str(e)}"
                self.quality_issues.append(error)
                logger.error(error)
        cursor.close()
    
    def check_duplicate_values(self, conn):
        """Check for duplicate values in unique fields"""
        unique_checks = [
            ('Customer', 'NationalID'),
            ('Customer', 'Username'),
        ]
        
        cursor = conn.cursor()
        for table, field in unique_checks:
            try:
                cursor.execute(f"""
                    SELECT {field}, COUNT(*) as cnt 
                    FROM {table} 
                    GROUP BY {field} 
                    HAVING COUNT(*) > 1
                """)
                duplicates = cursor.fetchall()
                if duplicates:
                    issue = f"Found {len(duplicates)} duplicate values in {table}.{field}"
                    self.quality_issues.append(issue)
                    logger.warning(issue)
            except Exception as e:
                error = f"Error checking duplicates in {table}.{field}: {str(e)}"
                self.quality_issues.append(error)
                logger.error(error)
        cursor.close()
    
    def check_referential_integrity(self, conn):
        """Check foreign key constraints"""
        fk_checks = [
            ('Device', 'CustomerID', 'Customer', 'CustomerID'),
            ('Account', 'CustomerID', 'Customer', 'CustomerID'),
            ('Transaction', 'FromAccountID', 'Account', 'AccountID'),
            ('Transaction', 'ToAccountID', 'Account', 'AccountID'),
            ('AuthenticationLog', 'CustomerID', 'Customer', 'CustomerID'),
            ('AuthenticationLog', 'DeviceID', 'Device', 'DeviceID')
        ]
        
        cursor = conn.cursor()
        for child_table, child_field, parent_table, parent_field in fk_checks:
            try:
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {child_table} c 
                    LEFT JOIN {parent_table} p ON c.{child_field} = p.{parent_field}
                    WHERE p.{parent_field} IS NULL AND c.{child_field} IS NOT NULL
                """)
                orphaned_count = cursor.fetchone()[0]
                if orphaned_count > 0:
                    issue = f"Found {orphaned_count} orphaned records in {child_table}.{child_field}"
                    self.quality_issues.append(issue)
                    logger.warning(issue)
            except Exception as e:
                error = f"Error checking FK {child_table}.{child_field}: {str(e)}"
                self.quality_issues.append(error)
                logger.error(error)
        cursor.close()
    
    def check_business_rules(self, conn):
        """Check business-specific rules"""
        cursor = conn.cursor()
        
        # Check for negative balances
        try:
            cursor.execute("SELECT COUNT(*) FROM Account WHERE Balance < 0")
            negative_balance_count = cursor.fetchone()[0]
            if negative_balance_count > 0:
                issue = f"Found {negative_balance_count} accounts with negative balance"
                self.quality_issues.append(issue)
                logger.warning(issue)
        except Exception as e:
            error = f"Error checking negative balances: {str(e)}"
            self.quality_issues.append(error)
            logger.error(error)
        
        # Check for transactions with zero amount
        try:
            cursor.execute("SELECT COUNT(*) FROM Transaction WHERE Amount <= 0")
            zero_amount_count = cursor.fetchone()[0]
            if zero_amount_count > 0:
                issue = f"Found {zero_amount_count} transactions with zero or negative amount"
                self.quality_issues.append(issue)
                logger.warning(issue)
        except Exception as e:
            error = f"Error checking zero amount transactions: {str(e)}"
            self.quality_issues.append(error)
            logger.error(error)
        
        # Check for future dates in transactions
        try:
            cursor.execute("SELECT COUNT(*) FROM Transaction WHERE Timestamp > NOW()")
            future_txn_count = cursor.fetchone()[0]
            if future_txn_count > 0:
                issue = f"Found {future_txn_count} transactions with future timestamps"
                self.quality_issues.append(issue)
                logger.warning(issue)
        except Exception as e:
            error = f"Error checking future transactions: {str(e)}"
            self.quality_issues.append(error)
            logger.error(error)
        
        cursor.close()
    
    def log_quality_issues(self, conn):
        """Log quality issues to a dedicated table"""
        if not self.quality_issues:
            return
            
        cursor = conn.cursor()
        
        # Create quality log table if it doesn't exist
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_quality_log (
                    log_id SERIAL PRIMARY KEY,
                    check_timestamp TIMESTAMP DEFAULT NOW(),
                    issue_count INTEGER,
                    issues_json TEXT,
                    status VARCHAR(20)
                )
            """)
            
            # Insert quality issues
            issues_json = json.dumps(self.quality_issues)
            cursor.execute("""
                INSERT INTO data_quality_log (issue_count, issues_json, status)
                VALUES (%s, %s, %s)
            """, (len(self.quality_issues), issues_json, 'FAILED'))
            
            conn.commit()
            logger.info(f"Logged {len(self.quality_issues)} quality issues to database")
            
        except Exception as e:
            logger.error(f"Error logging quality issues: {str(e)}")
        finally:
            cursor.close()
    
    def run_all_checks(self):
        """Run all data quality checks"""
        logger.info("Starting data quality checks...")
        
        conn = self.connect_db()
        try:
            self.check_null_values(conn)
            self.check_duplicate_values(conn)
            self.check_referential_integrity(conn)
            self.check_business_rules(conn)
            
            if self.quality_issues:
                logger.error(f"Data quality check FAILED with {len(self.quality_issues)} issues")
                self.log_quality_issues(conn)
                return False
            else:
                logger.info("Data quality check PASSED - no issues found")
                # Log successful check
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS data_quality_log (
                            log_id SERIAL PRIMARY KEY,
                            check_timestamp TIMESTAMP DEFAULT NOW(),
                            issue_count INTEGER,
                            issues_json TEXT,
                            status VARCHAR(20)
                        )
                    """)
                    cursor.execute("""
                        INSERT INTO data_quality_log (issue_count, issues_json, status)
                        VALUES (%s, %s, %s)
                    """, (0, '[]', 'PASSED'))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error logging successful check: {str(e)}")
                finally:
                    cursor.close()
                return True
                
        finally:
            conn.close()

if __name__ == "__main__":
    checker = DataQualityChecker()
    success = checker.run_all_checks()
    
    if not success:
        sys.exit(1)  # Exit with error code for Airflow
    else:
        sys.exit(0)  # Exit successfully
