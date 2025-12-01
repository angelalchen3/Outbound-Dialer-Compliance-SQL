-- outbound_dnc_flags_view.sql
-- Purpose:
--   Evaluate outbound dialing activity for rule-based contact frequency checks.
--   All schemas, field names, and business identifiers have been anonymized.

CREATE OR REPLACE VIEW analytics.mart.outbound_dnc_flags (
    interaction_date,
    interaction_time,
    contact_handled_flag,
    dialed_number,
    customer_id,
    contact_id,
    state_code,
    threshold_limit,
    daily_sequence,
    calls_last_7days,
    business_hours_pass,
    no_redial_after_answer_pass,
    state_daily_limit_pass,
    state_7day_limit_pass,
    other_state_limit_pass,
    system_disposition,
    postal_code,
    product_type,
    customer_first_name,
    customer_last_name,
    num_dials,
    campaign_name,
    queue_name,
    session_start_timestamp,
    interaction_id
) AS
WITH call_data AS (
    SELECT 
        o.interaction_date                         AS interaction_date,
        TO_CHAR(TO_TIMESTAMP(o.session_start), 
                'HH12:MI AM')                      AS interaction_time,
        o.session_start                            AS session_start_timestamp,
        o.contact_handled                          AS contact_handled_flag,
        o.dialed_number,
        c.customer_id,
        o.contact_id,
        c.state_code,
        o.system_disposition,
        c.postal_code,
        c.product_type,
        c.first_name              AS customer_first_name,
        c.last_name               AS customer_last_name,
        o.num_dials,
        o.campaign_name,
        o.queue_name,
        o.interaction_id,

        /* Row count per day per customer */
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_id, CAST(o.interaction_date AS DATE)
            ORDER BY o.session_start
        ) AS daily_sequence,

        /* 7-day lookback window start */
        CAST(o.interaction_date AS DATE) - INTERVAL '6 DAYS' AS window_start,

        /* Rolling count of calls in last 7 days */
        COUNT(*) OVER (
            PARTITION BY c.customer_id
            ORDER BY CAST(o.interaction_date AS DATE)
            RANGE BETWEEN INTERVAL '6 DAYS' PRECEDING 
                  AND CURRENT ROW
        ) AS calls_last_7days,

        /* Thresholds per state (generalized) */
        CASE 
            WHEN c.state_code = 'ST_A' THEN 7
            WHEN c.state_code = 'ST_B' THEN 7
            WHEN c.state_code = 'ST_C' THEN 2
            WHEN c.state_code = 'ST_D' THEN 30
            ELSE NULL
        END AS threshold_limit

    FROM analytics.source_outbound_dials o
    LEFT JOIN analytics.dim_customer_contact c
           ON o.contact_id = c.customer_id
          AND o.interaction_date = c.snapshot_date
    WHERE o.system_disposition NOT IN ('NUMBER_UNDIALABLE', 'BUSY_SIGNAL')
)

SELECT
    interaction_date,
    interaction_time,
    contact_handled_flag,
    dialed_number,
    customer_id,
    contact_id,
    state_code,
    threshold_limit,
    daily_sequence,
    calls_last_7days,

    /* Business hours pass: 8am–9pm */
    CASE 
        WHEN TIME(interaction_time) >= TIME '08:00:00'
         AND TIME(interaction_time) <  TIME '21:00:00'
        THEN 1 ELSE 0 
    END AS business_hours_pass,

    /* Cannot dial again if a transfer event occurred earlier in the day */
    CASE 
        WHEN MAX(
                CASE WHEN system_disposition IN ('TRANSFER_FLOW','TRANSFER_QUEUE')
                     THEN 1 ELSE 0 END
            ) OVER (
                PARTITION BY customer_id, CAST(interaction_date AS DATE)
                ORDER BY session_start_timestamp
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) = 1
        THEN 0 ELSE 1 
    END AS no_redial_after_answer_pass,

    /* State daily limits (generalized) */
    CASE WHEN state_code = 'ST_A' AND daily_sequence > 1 THEN 0 ELSE 1 END AS state_daily_limit_pass,

    /* 7-day rolling limits */
    CASE WHEN state_code = 'ST_A' AND calls_last_7days > threshold_limit THEN 0 ELSE 1 END AS state_7day_limit_pass,

    /* Generic rule for other states */
    CASE 
        WHEN state_code NOT IN ('ST_A','ST_B','ST_C','ST_D') 
         AND daily_sequence > 2 
        THEN 0 ELSE 1 
    END AS other_state_limit_pass,

    system_disposition,
    postal_code,
    product_type,
    customer_first_name,
    customer_last_name,
    num_dials,
    campaign_name,
    queue_name,
    session_start_timestamp,
    interaction_id
FROM call_data
ORDER BY 
    interaction_date DESC,
    interaction_time ASC;
