-- outbound_dnc_daily_summary.sql
-- Purpose:
--   Summarize daily contact-level compliance flags by date and state group.
--   Designed to be consumed by BI tools (e.g., Power BI) for monitoring.

SELECT
    interaction_date                                        AS interaction_date,
    COUNT(DISTINCT customer_id)                             AS distinct_customers,
    
    COUNT(DISTINCT CASE WHEN business_hours_pass = 0 
                        THEN customer_id END)               AS business_hours_fail_customers,

    COUNT(DISTINCT CASE WHEN no_redial_after_answer_pass = 0 
                        THEN customer_id END)               AS no_redial_after_answer_fail_customers,

    -- State-based daily / rolling limit failures (generalized)
    COUNT(DISTINCT CASE WHEN state_daily_limit_pass = 0 
                        AND state_code = 'ST_A'
                        THEN customer_id END)               AS st_a_daily_limit_fail_customers,

    COUNT(DISTINCT CASE WHEN state_7day_limit_pass = 0 
                        AND state_code = 'ST_A'
                        THEN customer_id END)               AS st_a_7day_limit_fail_customers,

    COUNT(DISTINCT CASE WHEN state_daily_limit_pass = 0 
                        AND state_code = 'ST_B'
                        THEN customer_id END)               AS st_b_daily_limit_fail_customers,

    COUNT(DISTINCT CASE WHEN state_7day_limit_pass = 0 
                        AND state_code = 'ST_B'
                        THEN customer_id END)               AS st_b_7day_limit_fail_customers,

    COUNT(DISTINCT CASE WHEN state_7day_limit_pass = 0 
                        AND state_code = 'ST_C'
                        THEN customer_id END)               AS st_c_7day_limit_fail_customers,

    COUNT(DISTINCT CASE WHEN state_7day_limit_pass = 0 
                        AND state_code = 'ST_D'
                        THEN customer_id END)               AS st_d_7day_limit_fail_customers,

    COUNT(DISTINCT CASE WHEN other_state_limit_pass = 0 
                        AND state_code NOT IN ('ST_A','ST_B','ST_C','ST_D')
                        THEN customer_id END)               AS other_state_limit_fail_customers,

    -- Counts of customers by state group
    COUNT(DISTINCT CASE WHEN state_code = 'ST_A' 
                        THEN customer_id END)               AS st_a_customers,

    COUNT(DISTINCT CASE WHEN state_code = 'ST_B' 
                        THEN customer_id END)               AS st_b_customers,

    COUNT(DISTINCT CASE WHEN state_code = 'ST_C' 
                        THEN customer_id END)               AS st_c_customers,

    COUNT(DISTINCT CASE WHEN state_code = 'ST_D' 
                        THEN customer_id END)               AS st_d_customers,

    COUNT(DISTINCT CASE WHEN state_code NOT IN ('ST_A','ST_B','ST_C','ST_D') 
                        THEN customer_id END)               AS other_state_customers

FROM analytics.mart.outbound_dnc_flags
GROUP BY
    interaction_date
ORDER BY
    interaction_date DESC;
