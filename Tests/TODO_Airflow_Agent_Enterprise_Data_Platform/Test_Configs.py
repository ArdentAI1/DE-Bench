import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Integrates CRM, E-commerce, and Marketing data into unified customer profiles with deduplication and real-time CDC.
2. Maps customer journeys, segments with ML, and models customer lifetime value using predictive analytics.
3. Harmonizes ERP and E-commerce transactions with validation, unified views, currency/tax handling, reconciliation, fraud monitoring, and GAAP reporting.
4. Integrates warehouse and 3PL inventory with real-time tracking, demand forecasting, multi-location optimization, automated reorder points, and KPI dashboards.
5. Tracks multichannel campaigns like email, social, and digital ads with multi-touch attribution, CAC/LTV, ROI cohorts, A/B testing, and predictive performance models.
6. Builds product analytics for performance scoring, recommendations, lifecycle analysis, competitive pricing, quality monitoring, and demand forecasting.
7. Delivers operational intelligence with real-time dashboards, supply-chain and fulfillment optimization, support analytics, quality control, and efficiency metrics.
8. Implements advanced ML for customer churn, sales forecasting, cross-sell recommendations, anomaly detection, sentiment analysis, and predictive maintenance.
9. Enforces governance with lineage, automated data-quality monitoring, GDPR anonymization, audit logging, retention policies, and access controls.
10. Ensures observability with comprehensive logging, quality and failure alerts, SLA dashboards, distributed tracing, health checks, and automated recovery.
11. Name the main DAG 'enterprise_data_platform_dag'.
12. Create it in branch 'feature/enterprise-data-platform'.
13. Name the PR 'Add Enterprise Data Platform with Advanced Analytics'.
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
