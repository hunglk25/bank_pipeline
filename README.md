# Banking Data Pipeline

A comprehensive data pipeline built with Apache Airflow for processing banking data with automated quality checks, error handling, and monitoring.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data          │    │   Quality        │    │   PostgreSQL    │
│   Generation    │───▶│   Checks         │───▶│   Storage       │
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Logging & Monitoring                         │
│  • Execution Logs  • Quality Reports  • Error Tracking         │
└─────────────────────────────────────────────────────────────────┘
```

## ✨ Features

- 🎯 **Synthetic Data Generation**: Creates realistic banking data (customers, accounts, transactions)
- 🔍 **Data Quality Checks**: Comprehensive validation with business rules
- 📊 **PostgreSQL Integration**: Robust data storage with error handling
- 📝 **Comprehensive Logging**: Database-backed logging and monitoring
- 🔄 **Automated Pipeline**: Airflow orchestration with retry logic
- 📧 **Error Notifications**: Email alerts on failures
- 🎛️ **Monitoring Dashboard**: Pipeline health and performance tracking

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- 8GB+ RAM recommended
- Ports 8080, 5432, 5433 available

### Setup & Run

1. **Clone and Setup**:

   ```bash
   git clone <your-repo>
   cd bank_pipeline
   chmod +x setup.sh
   ./setup.sh
   ```

2. **Access Airflow**:

   - Open http://localhost:8080
   - Login: `admin` / `admin`
   - Enable the `bank_data_pipeline` DAG
   - Trigger a manual run

3. **Monitor Progress**:
   - View real-time logs in Airflow UI
   - Check data quality results
   - Monitor error logs

## 📁 Project Structure

```
bank_pipeline/
├── dags/
│   ├── bank_data_pipeline.py      # Main Airflow DAG
│   └── etl.py                     # Alternative ETL implementation
├── src/
│   ├── generate_data_airflow.py   # Data generator
│   ├── data_quality_standards.py  # Quality checker
│   ├── data_uploader.py          # PostgreSQL uploader
│   └── pipeline_logger.py        # Centralized logging
├── config/
│   └── pipeline_config.py        # Configuration settings
├── sql/
│   ├── schema.sql                # Database schema
│   └── create_log_tables.sql     # Logging tables
├── docker-compose.yml            # Main services
├── docker-compose.override.yml   # Development overrides
├── requirements.txt              # Python dependencies
├── setup.sh                     # Setup automation
└── README.md                    # This file
```

## 🔧 Configuration

### Environment Variables (.env)

```bash
# Database
DB_HOST=postgres_data
DB_PORT=5432
DB_USER=user
DB_PASSWORD=userpass

# Data Generation
CUSTOMER_COUNT=10
TRANSACTION_COUNT=50

# Pipeline
LOG_LEVEL=INFO
PIPELINE_SCHEDULE=@daily
```

### Pipeline Configuration (config/pipeline_config.py)

- Database connections
- Data generation parameters
- Quality check rules
- Logging settings

## 📊 Data Pipeline Flow

### 1. Data Generation

- Creates synthetic customers with realistic profiles
- Generates multiple accounts per customer
- Creates transaction history
- Adds authentication logs

### 2. Quality Checks

- **Null Value Validation**: Critical fields must not be null
- **Duplicate Detection**: Identifies duplicate records
- **Referential Integrity**: Validates foreign key relationships
- **Business Rules**: Validates banking-specific logic
  - No negative balances
  - No zero-amount transactions
  - No future-dated transactions

### 3. Data Upload

- Uploads validated data to PostgreSQL
- Handles upload errors gracefully
- Logs failed records for investigation

### 4. Error Handling

- Logs all failures to dedicated tables
- Sends email notifications
- Provides detailed error tracking

## 📈 Monitoring & Logging

### Database Tables

- `pipeline_execution_log`: Task execution details
- `data_quality_log`: Quality check results
- `pipeline_error_log`: Error details and stack traces
- `data_upload_error_log`: Upload failure details
- `data_statistics_log`: Processing statistics

### Views

- `pipeline_health_summary`: 7-day health overview
- `data_quality_summary`: Quality trends

### Airflow UI Monitoring

- Real-time task status
- Log streaming
- Performance metrics
- Error notifications

## 🔍 Troubleshooting

### Common Issues

1. **Containers won't start**:

   ```bash
   docker-compose logs
   docker system prune -f
   ./setup.sh
   ```

2. **Database connection errors**:

   ```bash
   docker-compose exec postgres_data pg_isready -U user -d mydata
   ```

3. **Airflow DAG not visible**:

   - Check `/opt/airflow/dags` volume mount
   - Refresh Airflow UI
   - Check DAG syntax

4. **Quality checks failing**:
   - Check `data_quality_log` table
   - Review business rules in config
   - Validate source data

### Useful Commands

```bash
# View logs
docker-compose logs -f airflow-webserver
docker-compose logs -f postgres_data

# Restart services
docker-compose restart airflow-webserver airflow-scheduler

# Access database
docker-compose exec postgres_data psql -U user -d mydata

# Test components
docker-compose exec airflow-webserver python /opt/airflow/src/pipeline_logger.py
```

## 🎛️ Development

### Adding New Data Sources

1. Extend `generate_data_airflow.py`
2. Update `data_quality_standards.py`
3. Modify schema in `sql/schema.sql`
4. Add quality rules in `config/pipeline_config.py`

### Custom Quality Checks

1. Add check logic to `DataQualityChecker` class
2. Update configuration in `QUALITY_CONFIG`
3. Test with sample data

### New Notifications

1. Extend `PipelineLogger` class
2. Add notification logic to DAG tasks
3. Configure connection details

## 📝 API Reference

### PipelineLogger Class

```python
logger = PipelineLogger()

# Task logging
logger.log_task_start(run_id, dag_id, task_id, execution_date)
logger.log_task_success(run_id, dag_id, task_id, records_processed)
logger.log_task_failure(run_id, dag_id, task_id, error_message)

# Quality logging
logger.log_quality_check(run_id, table_name, check_type, status, issue_count, issues)

# Error logging
logger.log_error(run_id, dag_id, task_id, error_message, stack_trace)

# Monitoring
health = logger.get_pipeline_health(days=7)
errors = logger.get_recent_errors(hours=24)
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support, please:

1. Check the troubleshooting guide above
2. Review Airflow logs for specific errors
3. Check database connectivity
4. Create an issue with detailed error logs

---

**Happy Data Pipelining!** 🚀📊
