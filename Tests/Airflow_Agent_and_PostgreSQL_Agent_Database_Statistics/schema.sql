-- Create sample dataset for statistical analysis
CREATE TABLE dataset_samples (
    sample_id SERIAL PRIMARY KEY,
    category VARCHAR(50),
    measurement_value DECIMAL(15,4),
    measurement_date DATE,
    data_source VARCHAR(100),
    quality_score INTEGER CHECK (quality_score >= 1 AND quality_score <= 10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create time series data table
CREATE TABLE time_series_data (
    ts_id SERIAL PRIMARY KEY,
    time_period DATE,
    metric_name VARCHAR(100),
    metric_value DECIMAL(15,4),
    trend_indicator VARCHAR(20),
    seasonal_factor DECIMAL(8,4)
);

-- Insert sample data for comprehensive statistical analysis
INSERT INTO dataset_samples (category, measurement_value, measurement_date, data_source, quality_score) VALUES
-- Electronics category data (normal distribution around 150)
('Electronics', 145.25, '2024-01-01', 'sensor_a', 9),
('Electronics', 152.30, '2024-01-02', 'sensor_a', 8),
('Electronics', 148.75, '2024-01-03', 'sensor_a', 9),
('Electronics', 156.10, '2024-01-04', 'sensor_a', 8),
('Electronics', 144.80, '2024-01-05', 'sensor_a', 9),
('Electronics', 160.45, '2024-01-06', 'sensor_a', 7),
('Electronics', 147.90, '2024-01-07', 'sensor_a', 9),
('Electronics', 153.60, '2024-01-08', 'sensor_a', 8),
('Electronics', 149.30, '2024-01-09', 'sensor_a', 9),
('Electronics', 155.75, '2024-01-10', 'sensor_a', 8),

-- Furniture category data (different distribution around 85)
('Furniture', 82.15, '2024-01-01', 'sensor_b', 8),
('Furniture', 87.90, '2024-01-02', 'sensor_b', 9),
('Furniture', 84.60, '2024-01-03', 'sensor_b', 8),
('Furniture', 89.25, '2024-01-04', 'sensor_b', 9),
('Furniture', 86.40, '2024-01-05', 'sensor_b', 8),
('Furniture', 91.80, '2024-01-06', 'sensor_b', 7),
('Furniture', 83.70, '2024-01-07', 'sensor_b', 9),
('Furniture', 88.50, '2024-01-08', 'sensor_b', 8),
('Furniture', 85.95, '2024-01-09', 'sensor_b', 9),
('Furniture', 90.10, '2024-01-10', 'sensor_b', 8),

-- Office category data (higher variance around 200)
('Office', 185.30, '2024-01-01', 'sensor_c', 6),
('Office', 220.45, '2024-01-02', 'sensor_c', 7),
('Office', 195.80, '2024-01-03', 'sensor_c', 8),
('Office', 240.15, '2024-01-04', 'sensor_c', 6),
('Office', 175.90, '2024-01-05', 'sensor_c', 9),
('Office', 225.60, '2024-01-06', 'sensor_c', 7),
('Office', 190.25, '2024-01-07', 'sensor_c', 8),
('Office', 210.75, '2024-01-08', 'sensor_c', 7),
('Office', 180.40, '2024-01-09', 'sensor_c', 9),
('Office', 235.85, '2024-01-10', 'sensor_c', 6),

-- Kitchen category data (outliers and irregular distribution)
('Kitchen', 45.20, '2024-01-01', 'sensor_d', 5),
('Kitchen', 48.90, '2024-01-02', 'sensor_d', 6),
('Kitchen', 125.75, '2024-01-03', 'sensor_d', 4),  -- Outlier
('Kitchen', 47.60, '2024-01-04', 'sensor_d', 7),
('Kitchen', 46.80, '2024-01-05', 'sensor_d', 6),
('Kitchen', 142.30, '2024-01-06', 'sensor_d', 3),  -- Outlier
('Kitchen', 49.15, '2024-01-07', 'sensor_d', 7),
('Kitchen', 44.95, '2024-01-08', 'sensor_d', 8),
('Kitchen', 51.40, '2024-01-09', 'sensor_d', 6),
('Kitchen', 138.85, '2024-01-10', 'sensor_d', 4);  -- Outlier

-- Insert time series data for trend analysis
INSERT INTO time_series_data (time_period, metric_name, metric_value, trend_indicator, seasonal_factor) VALUES
('2024-01-01', 'daily_sales', 1250.75, 'increasing', 1.15),
('2024-01-02', 'daily_sales', 1318.90, 'increasing', 1.12),
('2024-01-03', 'daily_sales', 1275.60, 'stable', 1.08),
('2024-01-04', 'daily_sales', 1402.30, 'increasing', 1.20),
('2024-01-05', 'daily_sales', 1365.85, 'stable', 1.18),
('2024-01-06', 'daily_sales', 1195.40, 'decreasing', 0.95),
('2024-01-07', 'daily_sales', 1158.75, 'decreasing', 0.92),
('2024-01-08', 'daily_sales', 1290.65, 'increasing', 1.05),
('2024-01-09', 'daily_sales', 1345.20, 'increasing', 1.10),
('2024-01-10', 'daily_sales', 1425.95, 'increasing', 1.22),

('2024-01-01', 'customer_visits', 245.0, 'stable', 1.05),
('2024-01-02', 'customer_visits', 267.0, 'increasing', 1.15),
('2024-01-03', 'customer_visits', 234.0, 'decreasing', 0.95),
('2024-01-04', 'customer_visits', 289.0, 'increasing', 1.25),
('2024-01-05', 'customer_visits', 276.0, 'stable', 1.18),
('2024-01-06', 'customer_visits', 198.0, 'decreasing', 0.85),
('2024-01-07', 'customer_visits', 187.0, 'decreasing', 0.82),
('2024-01-08', 'customer_visits', 252.0, 'increasing', 1.08),
('2024-01-09', 'customer_visits', 271.0, 'increasing', 1.16),
('2024-01-10', 'customer_visits', 294.0, 'increasing', 1.28);

-- Comprehensive statistical analysis function
CREATE OR REPLACE FUNCTION calculate_comprehensive_statistics()
RETURNS void AS $$
BEGIN
    -- Drop existing statistics tables to ensure clean state
    DROP TABLE IF EXISTS descriptive_statistics CASCADE;
    DROP TABLE IF EXISTS category_comparisons CASCADE;
    DROP TABLE IF EXISTS correlation_analysis CASCADE;
    DROP TABLE IF EXISTS time_series_analysis CASCADE;
    DROP TABLE IF EXISTS outlier_detection CASCADE;
    DROP TABLE IF EXISTS quality_analysis CASCADE;
    
    -- 1. DESCRIPTIVE STATISTICS - Comprehensive statistical measures
    CREATE TABLE descriptive_statistics AS
    WITH statistical_base AS (
        SELECT 
            category,
            COUNT(*) as sample_size,
            SUM(measurement_value) as total_sum,
            AVG(measurement_value) as arithmetic_mean,
            -- Geometric mean (using natural log for numerical stability)
            EXP(AVG(LN(CASE WHEN measurement_value > 0 THEN measurement_value ELSE NULL END))) as geometric_mean,
            -- Harmonic mean
            1.0 / AVG(1.0 / NULLIF(measurement_value, 0)) as harmonic_mean,
            -- Central tendency measures
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY measurement_value) as median,
            MODE() WITHIN GROUP (ORDER BY measurement_value) as mode_value,
            -- Dispersion measures  
            STDDEV_POP(measurement_value) as population_stddev,
            STDDEV_SAMP(measurement_value) as sample_stddev,
            VAR_POP(measurement_value) as population_variance,
            VAR_SAMP(measurement_value) as sample_variance,
            -- Range and quartiles
            MAX(measurement_value) - MIN(measurement_value) as range_value,
            MIN(measurement_value) as minimum_value,
            MAX(measurement_value) as maximum_value,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY measurement_value) as q1_quartile,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY measurement_value) as q3_quartile,
            -- Additional percentiles
            PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY measurement_value) as p10_percentile,
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY measurement_value) as p90_percentile,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY measurement_value) as p95_percentile,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY measurement_value) as p99_percentile
        FROM dataset_samples
        GROUP BY category
    )
    SELECT 
        sb.*,
        -- Derived statistical measures
        sb.q3_quartile - sb.q1_quartile as interquartile_range,
        sb.sample_stddev / NULLIF(sb.arithmetic_mean, 0) as coefficient_of_variation,
        sb.p90_percentile - sb.p10_percentile as interdecile_range,
        -- Skewness approximation using Pearson's second skewness coefficient
        3 * (sb.arithmetic_mean - sb.median) / NULLIF(sb.sample_stddev, 0) as skewness_coefficient,
        -- Z-score boundaries for outlier detection (±2 standard deviations)
        sb.arithmetic_mean - 2 * sb.sample_stddev as lower_outlier_bound,
        sb.arithmetic_mean + 2 * sb.sample_stddev as upper_outlier_bound
    FROM statistical_base sb;
    
    -- 2. CATEGORY COMPARISONS - Statistical tests and comparisons
    CREATE TABLE category_comparisons AS
    WITH category_pairs AS (
        SELECT 
            c1.category as category_1,
            c2.category as category_2,
            c1.arithmetic_mean as mean_1,
            c2.arithmetic_mean as mean_2,
            c1.sample_stddev as stddev_1,
            c2.sample_stddev as stddev_2,
            c1.sample_size as n_1,
            c2.sample_size as n_2,
            ABS(c1.arithmetic_mean - c2.arithmetic_mean) as mean_difference,
            -- Effect size (Cohen's d approximation)
            ABS(c1.arithmetic_mean - c2.arithmetic_mean) / 
            SQRT((POWER(c1.sample_stddev, 2) + POWER(c2.sample_stddev, 2)) / 2.0) as cohens_d_effect_size
        FROM descriptive_statistics c1
        CROSS JOIN descriptive_statistics c2
        WHERE c1.category < c2.category  -- Avoid duplicate pairs
    )
    SELECT 
        cp.*,
        CASE 
            WHEN cp.cohens_d_effect_size < 0.2 THEN 'Small Effect'
            WHEN cp.cohens_d_effect_size < 0.5 THEN 'Medium Effect'
            ELSE 'Large Effect'
        END as effect_size_interpretation,
        -- F-ratio for variance comparison
        GREATEST(cp.stddev_1, cp.stddev_2) / NULLIF(LEAST(cp.stddev_1, cp.stddev_2), 0) as variance_ratio
    FROM category_pairs cp;
    
    -- 3. CORRELATION ANALYSIS - Relationship between measurement values and quality scores
    CREATE TABLE correlation_analysis AS
    WITH correlation_base AS (
        SELECT 
            category,
            COUNT(*) as sample_size,
            SUM(measurement_value) as sum_x,
            SUM(quality_score) as sum_y,
            SUM(measurement_value * quality_score) as sum_xy,
            SUM(POWER(measurement_value, 2)) as sum_x_squared,
            SUM(POWER(quality_score, 2)) as sum_y_squared,
            AVG(measurement_value) as mean_x,
            AVG(quality_score) as mean_y
        FROM dataset_samples
        GROUP BY category
    ),
    correlation_calc AS (
        SELECT 
            cb.*,
            -- Pearson correlation coefficient calculation
            (cb.sample_size * cb.sum_xy - cb.sum_x * cb.sum_y) / 
            NULLIF(SQRT((cb.sample_size * cb.sum_x_squared - POWER(cb.sum_x, 2)) * 
                       (cb.sample_size * cb.sum_y_squared - POWER(cb.sum_y, 2))), 0) as correlation_coefficient
        FROM correlation_base cb
    )
    SELECT 
        cc.*,
        -- Coefficient of determination (R²)
        POWER(cc.correlation_coefficient, 2) as r_squared,
        -- Correlation strength interpretation
        CASE 
            WHEN ABS(cc.correlation_coefficient) < 0.1 THEN 'No Correlation'
            WHEN ABS(cc.correlation_coefficient) < 0.3 THEN 'Weak Correlation'
            WHEN ABS(cc.correlation_coefficient) < 0.7 THEN 'Moderate Correlation'
            ELSE 'Strong Correlation'
        END as correlation_strength,
        -- Direction of correlation
        CASE 
            WHEN cc.correlation_coefficient > 0 THEN 'Positive'
            WHEN cc.correlation_coefficient < 0 THEN 'Negative'
            ELSE 'None'
        END as correlation_direction
    FROM correlation_calc cc;
    
    -- 4. TIME SERIES ANALYSIS - Trend and seasonal analysis
    CREATE TABLE time_series_analysis AS
    WITH time_series_stats AS (
        SELECT 
            metric_name,
            COUNT(*) as observation_count,
            AVG(metric_value) as mean_value,
            STDDEV(metric_value) as value_stddev,
            MIN(metric_value) as min_value,
            MAX(metric_value) as max_value,
            -- Trend analysis using linear regression slope approximation
            CORR(EXTRACT(EPOCH FROM time_period), metric_value) as trend_correlation,
            AVG(seasonal_factor) as mean_seasonal_factor,
            STDDEV(seasonal_factor) as seasonal_volatility,
            -- First and last values for growth calculation
            FIRST_VALUE(metric_value) OVER (PARTITION BY metric_name ORDER BY time_period) as first_value,
            LAST_VALUE(metric_value) OVER (PARTITION BY metric_name ORDER BY time_period 
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_value
        FROM time_series_data
        GROUP BY metric_name
    )
    SELECT 
        tss.*,
        -- Growth rate calculation
        (tss.last_value - tss.first_value) / NULLIF(tss.first_value, 0) * 100 as total_growth_rate_percent,
        -- Coefficient of variation
        tss.value_stddev / NULLIF(tss.mean_value, 0) as coefficient_of_variation,
        -- Trend interpretation
        CASE 
            WHEN tss.trend_correlation > 0.7 THEN 'Strong Upward Trend'
            WHEN tss.trend_correlation > 0.3 THEN 'Moderate Upward Trend'
            WHEN tss.trend_correlation < -0.7 THEN 'Strong Downward Trend'
            WHEN tss.trend_correlation < -0.3 THEN 'Moderate Downward Trend'
            ELSE 'No Clear Trend'
        END as trend_interpretation
    FROM time_series_stats tss;
    
    -- 5. OUTLIER DETECTION - Statistical outlier identification
    CREATE TABLE outlier_detection AS
    WITH outlier_analysis AS (
        SELECT 
            ds.sample_id,
            ds.category,
            ds.measurement_value,
            ds.quality_score,
            stat.arithmetic_mean,
            stat.sample_stddev,
            stat.q1_quartile,
            stat.q3_quartile,
            stat.interquartile_range,
            -- Z-score calculation
            (ds.measurement_value - stat.arithmetic_mean) / NULLIF(stat.sample_stddev, 0) as z_score,
            -- IQR-based outlier detection bounds
            stat.q1_quartile - 1.5 * stat.interquartile_range as iqr_lower_bound,
            stat.q3_quartile + 1.5 * stat.interquartile_range as iqr_upper_bound,
            -- Modified Z-score using median absolute deviation approximation
            ABS(ds.measurement_value - stat.median) / NULLIF(stat.sample_stddev, 0) as modified_z_score
        FROM dataset_samples ds
        JOIN descriptive_statistics stat ON ds.category = stat.category
    )
    SELECT 
        oa.*,
        -- Outlier classification
        CASE 
            WHEN ABS(oa.z_score) > 2 THEN 'Z-Score Outlier'
            WHEN oa.measurement_value < oa.iqr_lower_bound OR oa.measurement_value > oa.iqr_upper_bound THEN 'IQR Outlier'
            WHEN ABS(oa.modified_z_score) > 3.5 THEN 'Modified Z-Score Outlier'
            ELSE 'Normal'
        END as outlier_classification,
        -- Outlier severity
        CASE 
            WHEN ABS(oa.z_score) > 3 THEN 'Extreme Outlier'
            WHEN ABS(oa.z_score) > 2 THEN 'Moderate Outlier'
            ELSE 'Normal Range'
        END as outlier_severity
    FROM outlier_analysis oa;
    
    -- 6. QUALITY ANALYSIS - Data quality assessment
    CREATE TABLE quality_analysis AS
    SELECT 
        'overall' as scope,
        COUNT(*) as total_samples,
        COUNT(DISTINCT category) as unique_categories,
        AVG(quality_score) as avg_quality_score,
        STDDEV(quality_score) as quality_score_stddev,
        COUNT(CASE WHEN quality_score >= 8 THEN 1 END) as high_quality_samples,
        COUNT(CASE WHEN quality_score <= 5 THEN 1 END) as low_quality_samples,
        -- Completeness metrics
        COUNT(CASE WHEN measurement_value IS NOT NULL THEN 1 END) as complete_measurements,
        COUNT(CASE WHEN quality_score IS NOT NULL THEN 1 END) as complete_quality_scores,
        -- Outlier summary
        (SELECT COUNT(*) FROM outlier_detection WHERE outlier_classification != 'Normal') as total_outliers,
        (SELECT COUNT(*) FROM outlier_detection WHERE outlier_severity = 'Extreme Outlier') as extreme_outliers
    FROM dataset_samples;
    
    -- Create indexes for performance
    CREATE INDEX idx_descriptive_stats_category ON descriptive_statistics(category);
    CREATE INDEX idx_outlier_detection_classification ON outlier_detection(outlier_classification);
    CREATE INDEX idx_time_series_analysis_metric ON time_series_analysis(metric_name);
    
END;
$$ LANGUAGE plpgsql;

-- Function to get statistical summary report
CREATE OR REPLACE FUNCTION get_statistical_summary()
RETURNS TABLE(
    statistic_type VARCHAR(50),
    category_or_metric VARCHAR(100),
    statistic_name VARCHAR(100),
    statistic_value DECIMAL(15,6)
) AS $$
BEGIN
    RETURN QUERY
    -- Descriptive statistics summary
    SELECT 'Descriptive'::VARCHAR(50), category::VARCHAR(100), 'Mean'::VARCHAR(100), arithmetic_mean::DECIMAL(15,6) FROM descriptive_statistics
    UNION ALL
    SELECT 'Descriptive'::VARCHAR(50), category::VARCHAR(100), 'Standard Deviation'::VARCHAR(100), sample_stddev::DECIMAL(15,6) FROM descriptive_statistics
    UNION ALL
    SELECT 'Descriptive'::VARCHAR(50), category::VARCHAR(100), 'Coefficient of Variation'::VARCHAR(100), coefficient_of_variation::DECIMAL(15,6) FROM descriptive_statistics
    UNION ALL
    SELECT 'Descriptive'::VARCHAR(50), category::VARCHAR(100), 'Skewness'::VARCHAR(100), skewness_coefficient::DECIMAL(15,6) FROM descriptive_statistics
    UNION ALL
    -- Correlation analysis summary
    SELECT 'Correlation'::VARCHAR(50), category::VARCHAR(100), 'Pearson Correlation'::VARCHAR(100), correlation_coefficient::DECIMAL(15,6) FROM correlation_analysis
    UNION ALL
    SELECT 'Correlation'::VARCHAR(50), category::VARCHAR(100), 'R-Squared'::VARCHAR(100), r_squared::DECIMAL(15,6) FROM correlation_analysis
    UNION ALL
    -- Time series summary
    SELECT 'Time Series'::VARCHAR(50), metric_name::VARCHAR(100), 'Growth Rate %'::VARCHAR(100), total_growth_rate_percent::DECIMAL(15,6) FROM time_series_analysis
    UNION ALL
    SELECT 'Time Series'::VARCHAR(50), metric_name::VARCHAR(100), 'Trend Correlation'::VARCHAR(100), trend_correlation::DECIMAL(15,6) FROM time_series_analysis
    UNION ALL
    -- Quality metrics
    SELECT 'Quality'::VARCHAR(50), 'Overall'::VARCHAR(100), 'Average Quality Score'::VARCHAR(100), avg_quality_score::DECIMAL(15,6) FROM quality_analysis
    UNION ALL
    SELECT 'Quality'::VARCHAR(50), 'Overall'::VARCHAR(100), 'Total Outliers'::VARCHAR(100), total_outliers::DECIMAL(15,6) FROM quality_analysis;
END;
$$ LANGUAGE plpgsql;