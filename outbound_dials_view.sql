-- outbound_dials_view.sql
-- Purpose:
--   Standardize outbound dialing session data for analytics use.
--   All schemas, table names, and field names have been anonymized.

CREATE OR REPLACE VIEW analytics.mart.outbound_dials (
    load_timestamp,
    source_filename,
    campaign_name,
    interaction_id,
    interaction_date,
    contact_id,
    contact_list_name,
    session_start_timestamp,
    session_end_timestamp,
    queue_name,
    media_type,
    remote_display_name,
    system_disposition,
    contact_handled_flag,
    dialed_number,
    agent_name,
    wrapup_code,
    sip_response_code,
    num_dials,
    dialing_time_seconds,
    interring_time_seconds,
    session_duration_seconds,
    acw_time_seconds,
    time_to_abandon_seconds
) AS
SELECT
    load_datetime                 AS load_timestamp,
    file_name                     AS source_filename,
    campaign                      AS campaign_name,
    interaction_id,
    TO_DATE(interaction_date)     AS interaction_date,
    contact_id,
    contact_list_name,
    session_start                 AS session_start_timestamp,
    session_end                   AS session_end_timestamp,
    queue_name,
    media_type,
    remote_display_name,
    system_disposition,
    contact_handled               AS contact_handled_flag,
    dialed_number,
    agent_name,
    wrapup_code,
    sip_response_code,
    num_dials,
    dialing_time                  AS dialing_time_seconds,
    interring_time                AS interring_time_seconds,
    session_duration              AS session_duration_seconds,
    acw_time                      AS acw_time_seconds,
    time_to_abandon               AS time_to_abandon_seconds
FROM analytics.source_outbound_dials;
