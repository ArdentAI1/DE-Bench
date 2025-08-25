"""
Configuration for PostgreSQL Database-Side Statistical Analysis Test

This test demonstrates moving complex statistical computations from pandas/Python
to pure SQL, leveraging PostgreSQL's advanced analytical functions for optimal performance.
"""

# Task description for the AI agent
User_Input = """
Create an Airflow DAG that:
1. Creates a stored procedure that performs a comprehensive statistical analysis of a database.
2. Expected statistical outputs (all created database-side):
   - **descriptive_statistics**: Comprehensive statistical measures per category
     * Arithmetic, geometric, harmonic means
     * Standard deviation, variance, coefficient of variation
     * Quartiles, percentiles, range, interquartile range
     * Skewness coefficient, outlier bounds
   - **category_comparisons**: Statistical comparisons between categories
     * Effect sizes (Cohen's d)
     * Variance ratios, mean differences
   - **correlation_analysis**: Pearson correlation coefficients and R-squared values
   - **time_series_analysis**: Trend analysis with growth rates and seasonal factors
   - **outlier_detection**: Statistical outlier identification using Z-scores and IQR methods
   - **quality_analysis**: Data quality assessment and completeness metrics

6. Advanced SQL statistical techniques to implement:
   - Pearson correlation coefficient calculation using SQL aggregates
   - Z-score and modified Z-score calculations
   - Percentile and quartile computations using PERCENTILE_CONT
   - Linear regression slope approximation using CORR function
   - Coefficient of variation and skewness measures
   - IQR-based outlier detection with mathematical bounds
   - Effect size calculations (Cohen's d) for group comparisons

7. Performance benefits validation:
   - All computation happens in PostgreSQL engine (zero data movement)
   - Leverages database mathematical functions and optimizations
   - Uses SQL window functions for efficient statistical operations
   - Minimal Airflow resource usage

The database already contains the comprehensive statistical analysis function - your DAG should execute it using a PostgreSQLOperator.

Focus on demonstrating that complex statistical analysis traditionally done with pandas can be performed more efficiently using pure SQL with PostgreSQL's advanced analytical capabilities.

Validate that statistical measures are mathematically accurate and demonstrate the performance benefits of database-side computation over container-based processing.
"""

# Configuration for database and services
Configs = {
    "services": {
        "postgreSQL": {
            "hostname": "POSTGRES_HOSTNAME",
            "port": "POSTGRES_PORT",
            "username": "POSTGRES_USERNAME",
            "password": "POSTGRES_PASSWORD", 
            "databases": []  # Will be populated by test fixture
        },
        "airflow": {
            "host": "",  # Will be populated by test fixture
            "username": "",  # Will be populated by test fixture
            "password": "",  # Will be populated by test fixture
            "api_token": "",  # Will be populated by test fixture
            "dag_folder": "dags/"
        }
    },
    "test_metadata": {
        "difficulty": 5,
        "categories": ["database", "sql", "statistics", "mathematical_analysis", "correlation", "time_series"],
        "computation_location": "database",  # Key difference from pandas-based tests
        "sql_statistical_functions": [
            "PERCENTILE_CONT for quartiles and percentiles",
            "STDDEV/VAR for variance and standard deviation",
            "CORR for Pearson correlation coefficient", 
            "Mathematical functions (EXP, LN, SQRT, POWER)",
            "Window functions for ranking and partitioning",
            "Statistical aggregates (AVG, SUM, COUNT)",
            "Complex CTEs for multi-step calculations"
        ],
        "statistical_measures": [
            "Descriptive statistics (mean, median, mode, range)",
            "Dispersion measures (variance, standard deviation, CV)",
            "Distribution measures (skewness, quartiles, percentiles)",
            "Correlation analysis (Pearson r, R-squared)",
            "Effect size calculations (Cohen's d)",
            "Outlier detection (Z-scores, IQR method)",
            "Time series analysis (trends, growth rates)",
            "Data quality assessment"
        ],
        "performance_benefits": [
            "Zero data movement - all computation in database",
            "Leverages PostgreSQL's optimized mathematical functions",
            "Uses database indexes and query optimization",
            "Eliminates pandas memory overhead in Airflow",
            "Better scalability for large statistical datasets",
            "Native SQL statistical accuracy and precision"
        ],
        "pandas_alternatives": [
            "df.describe() → SQL descriptive statistics with PERCENTILE_CONT",
            "df.corr() → SQL Pearson correlation using CORR function",
            "df.std() → SQL STDDEV function",
            "df.quantile() → SQL PERCENTILE_CONT window function",
            "scipy.stats → SQL mathematical functions and CTEs",
            "outlier detection → SQL Z-score and IQR calculations"
        ]
    }
}