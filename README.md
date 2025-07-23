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
Data Generation → Quality Checks → PostgreSQL Storage → Visualization & Monitoring

## Features
- Synthetic banking data generation
- Comprehensive data quality validation
- PostgreSQL data storage
- Real-time Streamlit dashboard
- Risk monitoring and compliance
- Airflow pipeline orchestration

## How to Run
1. Start services:
   ```bash
   docker-compose up -d
   ```

2. Access applications:
   - **Airflow**: http://localhost:8081 (admin/admin)
   - **pgAdmin**: http://localhost:8082 (admin@admin.com/admin)
   - **Streamlit Dashboard**: http://localhost:8501

3. Run pipeline:
   - Enable `bank_data_pipeline` DAG in Airflow
   - Trigger manual run or wait for scheduled execution

## Access Information
- **Airflow**: http://localhost:8081
  - Username: admin
  - Password: admin

- **pgAdmin**: http://localhost:8082
  - Email: admin@admin.com
  - Password: admin

- **Streamlit**: http://localhost:8501

- **PostgreSQL Database**:
  - Host: localhost:5433
  - Username: user
  - Password: userpass
  - Database: mydata
