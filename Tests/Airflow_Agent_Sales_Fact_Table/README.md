# Airflow Agent Sales Fact Table Creation Test

This test validates that an AI agent can create an Airflow DAG that builds a sales fact table with proper foreign key relationships from three source tables.

## Test Overview

The test verifies that an AI agent can:
1. Create an Airflow DAG that reads from three PostgreSQL tables (transactions, items, customers)
2. Generate a sales fact table with proper foreign key constraints
3. Create realistic sales data that demonstrates the relationships between the source tables
4. Ensure data integrity through foreign key validation

## Database Schema

### Source Tables

**transactions**
- `transaction_id` (SERIAL PRIMARY KEY)
- `transaction_date` (DATE)
- `total_amount` (DECIMAL)
- `payment_method` (VARCHAR)
- `store_location` (VARCHAR)

**items**
- `item_id` (SERIAL PRIMARY KEY)
- `item_name` (VARCHAR)
- `category` (VARCHAR)
- `unit_price` (DECIMAL)
- `supplier` (VARCHAR)

**customers**
- `customer_id` (SERIAL PRIMARY KEY)
- `first_name` (VARCHAR)
- `last_name` (VARCHAR)
- `email` (VARCHAR)
- `phone` (VARCHAR)
- `address` (TEXT)

### Target Table

**sales_fact**
- `sales_id` (SERIAL PRIMARY KEY)
- `transaction_id` (FOREIGN KEY to transactions.transaction_id)
- `item_id` (FOREIGN KEY to items.item_id)
- `customer_id` (FOREIGN KEY to customers.customer_id)
- `quantity` (INTEGER)
- `unit_price` (DECIMAL)
- `total_amount` (DECIMAL)
- `sale_date` (DATE)

## Test Validation

The test performs the following validations:

1. **Table Creation**: Verifies that the sales_fact table exists and contains data
2. **Schema Validation**: Checks that all expected columns are present with correct data types
3. **Foreign Key Constraints**: Validates that proper foreign key constraints exist for:
   - `transaction_id` → `transactions.transaction_id`
   - `item_id` → `items.item_id`
   - `customer_id` → `customers.customer_id`
4. **Data Integrity**: Ensures no orphaned records exist (all foreign keys reference valid records)
5. **Business Logic**: Verifies that `total_amount = quantity * unit_price`
6. **Relationship Validation**: Confirms that sales records properly link transactions, items, and customers

## Test Steps

1. **Setup**: Initialize PostgreSQL database with source tables and sample data
2. **Model Execution**: Run the AI agent to create the Airflow DAG and GitHub PR
3. **DAG Execution**: Trigger and monitor the Airflow DAG execution
4. **Validation**: Verify the sales_fact table creation and foreign key relationships
5. **Cleanup**: Remove test resources and configurations

## Expected Outcome

The test should successfully:
- Create a sales_fact table with proper foreign key constraints
- Generate realistic sales data that demonstrates relationships between source tables
- Maintain referential integrity across all foreign key relationships
- Pass all validation checks for data quality and business logic

## Dependencies

- Airflow instance (via fixture)
- GitHub repository (via fixture)
- PostgreSQL database (via fixture)
- Supabase account (via fixture)

## Tags

- `@pytest.mark.airflow`
- `@pytest.mark.pipeline`
- `@pytest.mark.database`
- `@pytest.mark.three` (Difficulty level 3)
