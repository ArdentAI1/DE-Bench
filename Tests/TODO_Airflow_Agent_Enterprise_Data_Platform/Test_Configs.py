import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an enterprise-grade Airflow DAG that implements a comprehensive data platform for a multi-billion dollar company with the following complex requirements:

1. CUSTOMER 360 DATA INTEGRATION:
   - Integrate customer data from 3 different systems (CRM, E-commerce, Marketing Automation)
   - Create a unified customer profile with deduplication and data quality scoring
   - Implement customer journey mapping across all touchpoints
   - Build customer lifetime value (CLV) models with predictive analytics
   - Create customer segmentation using machine learning clustering algorithms
   - Implement real-time customer data updates with change data capture (CDC)

2. MULTI-SOURCE TRANSACTION PROCESSING:
   - Process transactions from ERP and E-commerce systems with different schemas
   - Implement data validation and business rule enforcement
   - Create unified transaction views with currency conversion and tax calculations
   - Build real-time transaction monitoring and fraud detection
   - Implement transaction reconciliation across systems
   - Create financial reporting with GAAP compliance

3. INVENTORY OPTIMIZATION PLATFORM:
   - Integrate inventory data from internal warehouses and 3PL systems
   - Implement real-time inventory tracking with IoT sensor data simulation
   - Create demand forecasting models using time series analysis
   - Build inventory optimization algorithms for multi-location fulfillment
   - Implement automated reorder point calculations
   - Create inventory performance dashboards with KPIs

4. MARKETING ATTRIBUTION AND ROI:
   - Track marketing campaigns across multiple channels (Email, Social, Digital Ads)
   - Implement multi-touch attribution modeling
   - Calculate customer acquisition cost (CAC) and lifetime value (LTV)
   - Create marketing ROI dashboards with cohort analysis
   - Implement A/B testing framework for campaign optimization
   - Build predictive models for campaign performance

5. PRODUCT ANALYTICS AND PERFORMANCE:
   - Create product performance scoring using multiple metrics
   - Implement product recommendation engines
   - Build product lifecycle analytics
   - Create competitive pricing analysis
   - Implement product quality monitoring with customer feedback
   - Build product demand forecasting models

6. OPERATIONAL INTELLIGENCE:
   - Create real-time operational dashboards
   - Implement supply chain optimization algorithms
   - Build shipping and fulfillment optimization
   - Create customer support analytics and ticket routing
   - Implement quality control monitoring
   - Build operational efficiency metrics

7. ADVANCED ANALYTICS AND MACHINE LEARNING:
   - Implement customer churn prediction models
   - Create sales forecasting using ensemble methods
   - Build recommendation systems for cross-selling
   - Implement anomaly detection for fraud and quality issues
   - Create sentiment analysis for customer feedback
   - Build predictive maintenance models for inventory

8. DATA GOVERNANCE AND COMPLIANCE:
   - Implement data lineage tracking across all transformations
   - Create data quality monitoring with automated alerts
   - Build GDPR compliance framework with data anonymization
   - Implement audit logging for all data access and changes
   - Create data retention policies and automated cleanup
   - Build data security monitoring and access controls

9. REAL-TIME STREAMING PROCESSING:
   - Implement real-time data ingestion from multiple sources
   - Create streaming analytics for immediate business insights
   - Build real-time alerting for critical business events
   - Implement event-driven architecture for data processing
   - Create real-time dashboards with sub-second latency
   - Build streaming ETL pipelines for operational data

10. ENTERPRISE DATA ARCHITECTURE:
    - Implement data lake to data warehouse architecture
    - Create data marts for different business units
    - Build data virtualization layer for real-time access
    - Implement data catalog and metadata management
    - Create data API layer for application integration
    - Build data mesh architecture for domain-driven design

11. PERFORMANCE AND SCALABILITY:
    - Implement parallel processing for large datasets
    - Create incremental processing for efficiency
    - Build data partitioning and optimization strategies
    - Implement caching layers for frequently accessed data
    - Create performance monitoring and optimization
    - Build auto-scaling capabilities for variable workloads

12. MONITORING AND OBSERVABILITY:
    - Implement comprehensive logging and monitoring
    - Create alerting for data quality and pipeline failures
    - Build performance dashboards and SLA monitoring
    - Implement distributed tracing for data lineage
    - Create health checks and automated recovery
    - Build capacity planning and resource optimization

PIPELINE REQUIREMENTS:
- Use multiple DAGs with complex dependencies and orchestration
- Implement error handling, retry logic, and circuit breakers
- Create data validation checkpoints and quality gates
- Build idempotent operations for data consistency
- Implement data versioning and rollback capabilities
- Create automated testing and deployment pipelines
- Run on a schedule with real-time triggers
- Name the main DAG 'enterprise_data_platform_dag'
- Create it in a branch called 'feature/enterprise-data-platform'
- Name the PR 'Add Enterprise Data Platform with Advanced Analytics'
- Use modern data engineering tools (pandas, numpy, scikit-learn, etc.)
- Implement proper logging, monitoring, and alerting
- Create comprehensive documentation and runbooks
- Build disaster recovery and backup procedures
- Implement security best practices and encryption
- Create data governance and compliance frameworks
"""

Configs = {
    "services": {
        "airflow": {
            "github_token": os.getenv("AIRFLOW_GITHUB_TOKEN"),
            "repo": os.getenv("AIRFLOW_REPO"),
            "dag_path": os.getenv("AIRFLOW_DAG_PATH"),
            "requirements_path": os.getenv("AIRFLOW_REQUIREMENTS_PATH"),
            "host": os.getenv("AIRFLOW_HOST", "http://localhost:8080"),
            "username": os.getenv("AIRFLOW_USERNAME", "airflow"),
            "password": os.getenv("AIRFLOW_PASSWORD", "airflow"),
            "api_token": os.getenv("AIRFLOW_API_TOKEN"),
        },
        "postgreSQL": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "port": os.getenv("POSTGRES_PORT"),
            "username": os.getenv("POSTGRES_USERNAME"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "databases": [{"name": "enterprise_platform"}],  # Will be updated with actual database name from fixture
        }
    }
}
