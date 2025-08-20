# Advanced Data Engineering Pipeline Test

This test validates that an AI agent can create a comprehensive data engineering pipeline that demonstrates advanced data engineering concepts including data cleansing, transformation, analytics, and monitoring.

## Test Overview

This is a **Difficulty Level 4** test that simulates a real-world data engineering scenario where an AI agent must:

1. **Process messy, inconsistent raw data** with data quality issues
2. **Implement comprehensive data cleansing and validation** 
3. **Create multiple data transformation layers** with business logic
4. **Build analytical models** for inventory, customer, and sales analysis
5. **Implement data quality monitoring** and alerting systems
6. **Establish data lineage** and audit trails
7. **Create business intelligence dashboards** with KPIs

## Database Schema

### Source Tables (Raw Data)

**raw_orders** - Contains messy order data with quality issues:
- Inconsistent date formats
- Invalid email addresses
- Negative quantities
- Missing product references
- Inconsistent customer names

**raw_inventory** - Inventory movement data:
- Stock in/out movements
- Adjustments and returns
- Cost tracking
- Warehouse locations

**raw_customer_feedback** - Customer reviews and ratings:
- Product ratings and reviews
- Sentiment scores
- Feedback categorization

**product_catalog** - Master product data:
- Product details and categorization
- Cost and retail pricing
- Supplier information
- Physical specifications

### Target Tables (Transformed Data)

**cleaned_orders** - Validated and standardized order data:
- Data quality flags
- Standardized formats
- Enriched with product details
- Profit margin calculations

**customer_dimension** - Deduplicated customer master data:
- Unique customer records
- Standardized contact information
- Customer attributes

**inventory_facts** - Daily inventory positions:
- Current stock levels
- Inventory turnover rates
- Days of inventory
- Inventory value calculations

**customer_sentiment** - Aggregated customer feedback:
- Average ratings by product/category
- Sentiment trend analysis
- Customer satisfaction metrics

**sales_facts** - Comprehensive sales metrics:
- Sales performance data
- Revenue and profit analysis
- Product performance metrics

**product_performance** - Product analytics:
- Sales, inventory, and feedback metrics
- Performance rankings
- Trend analysis

**daily_sales_summary** - Aggregated KPIs:
- Daily revenue and order counts
- Average order values
- Performance trends

**customer_behavior_analysis** - Customer segmentation:
- Customer lifetime value
- Purchase behavior patterns
- Customer segments

**data_quality_metrics** - Data quality monitoring:
- Completeness scores
- Accuracy metrics
- Consistency checks
- Quality trend tracking

## Test Validation

The test performs comprehensive validation across 8 key areas:

### 1. Data Cleansing and Validation
- Verifies data quality checks and validation logic
- Confirms invalid records are properly filtered
- Validates data quality flags and status indicators
- Checks email validation and date format standardization

### 2. Data Transformation Tables
- Validates cleaned_orders table creation and enrichment
- Verifies customer dimension table with deduplication
- Checks profit margin calculations and business logic
- Confirms data enrichment with product catalog

### 3. Inventory Analysis
- Verifies inventory_facts table with stock calculations
- Validates inventory turnover rate calculations
- Checks days of inventory metrics
- Confirms inventory value and cost tracking

### 4. Customer Analytics
- Validates customer_sentiment table creation
- Verifies sentiment score calculations and aggregation
- Checks customer segmentation logic
- Confirms customer lifetime value calculations

### 5. Business Intelligence Tables
- Verifies sales_facts table with comprehensive metrics
- Validates product_performance table creation
- Checks daily_sales_summary with KPIs
- Confirms customer_behavior_analysis table

### 6. Data Quality Monitoring
- Validates data_quality_metrics table creation
- Verifies quality score calculations
- Checks audit logging implementation
- Confirms monitoring and alerting setup

### 7. Data Lineage and Relationships
- Verifies proper foreign key relationships
- Validates data flow between tables
- Checks referential integrity
- Confirms business logic consistency

### 8. Comprehensive Business Logic
- Validates calculation accuracy (e.g., total_amount = quantity * unit_price)
- Checks profit margin calculations
- Verifies inventory turnover formulas
- Confirms customer segmentation logic

## Advanced Data Engineering Concepts Tested

### Data Quality Management
- **Completeness**: Missing value detection and handling
- **Accuracy**: Data validation and business rule enforcement
- **Consistency**: Cross-table validation and standardization
- **Timeliness**: Data freshness and processing latency

### Data Transformation Patterns
- **ETL/ELT**: Extract, transform, load processes
- **Data Cleansing**: Standardization and validation
- **Data Enrichment**: Joining with master data
- **Aggregation**: Summarization and KPI calculation

### Business Intelligence
- **Dimensional Modeling**: Star schema implementation
- **Fact Tables**: Transaction and event data
- **Dimension Tables**: Master and reference data
- **KPIs**: Key performance indicators and metrics

### Data Governance
- **Data Lineage**: Tracking data flow and transformations
- **Audit Logging**: Change tracking and compliance
- **Quality Monitoring**: Automated quality checks
- **Alerting**: Proactive issue detection

### Advanced Analytics
- **Customer Segmentation**: Behavioral analysis and clustering
- **Sentiment Analysis**: Text processing and scoring
- **Inventory Analytics**: Turnover and optimization
- **Performance Metrics**: Multi-dimensional analysis

## Test Complexity Factors

This test is significantly more complex than the previous examples because it:

1. **Multiple Data Sources**: Processes 4 different source tables with different schemas
2. **Data Quality Issues**: Handles real-world data problems (invalid emails, negative quantities, etc.)
3. **Complex Transformations**: Implements business logic, calculations, and aggregations
4. **Multiple Output Tables**: Creates 8+ target tables with different purposes
5. **Advanced Analytics**: Includes sentiment analysis, segmentation, and performance metrics
6. **Data Governance**: Implements monitoring, logging, and quality tracking
7. **Business Logic**: Validates complex calculations and relationships
8. **Error Handling**: Requires robust error handling and validation

## Expected Outcome

The test should successfully demonstrate:
- **Comprehensive data pipeline** with multiple transformation stages
- **Data quality management** with validation and monitoring
- **Business intelligence** with actionable metrics and KPIs
- **Data governance** with lineage tracking and audit trails
- **Advanced analytics** with customer and inventory insights
- **Production-ready pipeline** with error handling and monitoring

## Dependencies

- Airflow instance (via fixture)
- GitHub repository (via fixture)
- PostgreSQL database (via fixture)
- Supabase account (via fixture)

## Tags

- `@pytest.mark.airflow`
- `@pytest.mark.pipeline`
- `@pytest.mark.database`
- `@pytest.mark.four` (Difficulty level 4 - Advanced)

## Real-World Relevance

This test simulates the type of complex data engineering work that senior data engineers perform in production environments, including:

- **Data Lake to Data Warehouse** transformations
- **Multi-source data integration** and consolidation
- **Business intelligence** and analytics platform development
- **Data quality** and governance implementation
- **Performance optimization** and monitoring
- **Compliance** and audit requirements
