# Banking Data Pipeline

## Project Structure
```
├── dags/                          # Airflow DAGs
│   └── bank_data_pipeline2.py    # Main pipeline DAG
├── src/                           # Core pipeline components
│   ├── generate_data.py          # Data generator
│   ├── data_quality.py           # Quality validator
│   ├── data_uploader.py          # Database uploader
│   ├── monitoring_audit.py       # Risk monitoring
│   └── pipeline_logger.py        # Logging utilities
├── visualization/                 # Dashboard
│   └── dashboard.py              # Streamlit dashboard
├── sql/                          # Database schema
│   └── schema.sql                # PostgreSQL tables
├── config/                       # Configuration
│   └── pipeline_config.py        # Pipeline settings
├── docker-compose.yml            # Container orchestration
└── requirements.txt              # Python dependencies
```

## Architecture
Data Generation → Quality Checks → PostgreSQL Storage → Risk Monitoring → Visualization

## Features
- Synthetic banking data generation (customers, devices, accounts, transactions, auth logs)
- Comprehensive data quality validation with database checks
- PostgreSQL data storage with proper schema
- Real-time Streamlit dashboard with interactive charts
- Risk monitoring and compliance checking
- Airflow pipeline orchestration with error handling

## Database Schema
The system uses PostgreSQL with the following core tables:
- **Customer**: Customer information with NationalID uniqueness
- **Device**: Customer devices with verification status
- **Account**: Customer accounts with balance tracking
- **Transaction**: Financial transactions with amounts and timestamps
- **AuthenticationLog**: Authentication attempts and methods
- **RiskViolations**: Risk monitoring alerts and violations

## Data Quality Checks
The pipeline performs comprehensive quality validation:
- **Null/Missing Values**: Validates critical ID fields (CustomerID, NationalID, Username)
- **Uniqueness Constraints**: Checks NationalID and Username duplicates in data and database
- **Format Validation**: CCCD format (12 digits) and Passport format (A12345678)
- **Foreign Key Integrity**: Ensures referential integrity between tables
- **Database Existence**: Prevents duplicate NationalID/Username in existing database

Quality issues are logged and failed records are saved to `failed_records.json`

## Risk Monitoring
Automated risk detection with 3 core rules:
- **RULE_001**: High value transactions (>10M VND) without strong authentication (BIOMETRIC)
- **RULE_002**: Transactions from unverified devices
- **RULE_003**: Daily transaction limits exceeded (>20M VND) without strong authentication

## How to Run
1. Start services:
   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```


2. Access applications:
   - **Airflow**: http://localhost:8080 (admin/admin)
   - **pgAdmin**: http://localhost:8082 (admin@admin.com/admin)
   - **Streamlit Dashboard**: http://localhost:8501

3. Run pipeline:
   - Enable `bank_data_pipeline` DAG in Airflow
   - Trigger manual run or wait for scheduled execution

## Access Information
- **Airflow**: http://localhost:8080
  - Username: admin
  - Password: admin

- **pgAdmin**: http://localhost:8082
  - Email: admin@admin.com
  - Password: admin

- **Streamlit**: http://localhost:8501

- **PostgreSQL Database**:
  - Host: localhost:5432
  - Username: user
  - Password: userpass