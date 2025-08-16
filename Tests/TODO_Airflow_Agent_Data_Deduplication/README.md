# Airflow Agent Data Deduplication Test

This test validates an Airflow agent's ability to create a data deduplication pipeline that consolidates user information from multiple source tables into a single deduplicated table.

## Test Overview

The test:
1. Sets up three PostgreSQL tables with user data containing unique and duplicate records
2. Instructs the AI agent to create an Airflow DAG for data deduplication
3. Validates that the DAG successfully deduplicates data based on email addresses
4. Verifies the deduplicated table structure and data integrity

## Test Data Setup

The test creates three source tables with overlapping user data:

### users_source_1
- Contains: john.doe, jane.smith, bob.wilson, alice.johnson
- Columns: id, email, first_name, last_name, company, source

### users_source_2  
- Contains: john.doe (duplicate), sarah.brown, mike.davis, lisa.garcia
- Columns: id, email, first_name, last_name, department, source

### users_source_3
- Contains: jane.smith (duplicate), bob.wilson (duplicate), emma.taylor, david.lee
- Columns: id, email, first_name, last_name, role, source

## Expected Deduplication Results

The deduplicated table should contain 9 unique users:
- john.doe@example.com (from source_1 and source_2)
- jane.smith@example.com (from source_1 and source_3)
- bob.wilson@example.com (from source_1 and source_3)
- alice.johnson@example.com (from source_1 only)
- sarah.brown@example.com (from source_2 only)
- mike.davis@example.com (from source_2 only)
- lisa.garcia@example.com (from source_2 only)
- emma.taylor@example.com (from source_3 only)
- david.lee@example.com (from source_3 only)

## Validation

The test validates:
- ✅ DAG creation and deployment via GitHub PR
- ✅ Successful DAG execution in Airflow
- ✅ Correct table structure with all expected columns
- ✅ Proper deduplication (no duplicate email addresses)
- ✅ Data integrity (correct number of unique users)
- ✅ Merged data from multiple sources for duplicate users

## Running the Test

```bash
pytest Tests/Airflow_Agent_Data_Deduplication/test_airflow_agent_data_deduplication.py -v
```

## Environment Requirements

- `AIRFLOW_GITHUB_TOKEN`: GitHub token for Airflow repository access
- `AIRFLOW_REPO`: GitHub repository for Airflow DAGs
- `AIRFLOW_DAG_PATH`: Path to DAGs directory in repository
- `AIRFLOW_REQUIREMENTS_PATH`: Path to requirements.txt in repository
- `AIRFLOW_HOST`: Airflow host URL
- `AIRFLOW_USERNAME`: Airflow username
- `AIRFLOW_PASSWORD`: Airflow password
- `AIRFLOW_API_TOKEN`: Airflow API token
- `POSTGRES_HOSTNAME`: PostgreSQL host
- `POSTGRES_PORT`: PostgreSQL port
- `POSTGRES_USERNAME`: PostgreSQL username
- `POSTGRES_PASSWORD`: PostgreSQL password
- `ASTRO_ACCESS_TOKEN`: Astro access token for deployment

## What This Test Validates

1. **Agent Capability**: Ability to create complex data deduplication pipelines using Airflow
2. **Technical Integration**: Integration between Airflow, PostgreSQL, and GitHub for CI/CD
3. **Real-world Scenario**: Common data engineering task of consolidating user data from multiple sources
4. **Data Quality**: Ensuring data integrity through proper deduplication logic
5. **Pipeline Orchestration**: Creating scheduled, automated data processing workflows

## Test Difficulty

This test is marked as difficulty level 3 because it involves:
- Complex data transformation logic (deduplication)
- Multiple data source integration
- Data quality validation
- Pipeline orchestration with scheduling
- Real-world data engineering patterns
