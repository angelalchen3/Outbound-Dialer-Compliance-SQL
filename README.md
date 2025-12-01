# Outbound Dialer Compliance – SQL Models

This repository contains anonymized SQL models used to evaluate outbound dialing activity against rule-based contact frequency and timing checks. The focus is on demonstrating how to transform raw dialer logs into analytics-ready views for compliance monitoring and reporting (e.g., in Power BI).

All schemas, table names, state codes, and identifiers have been anonymized. No proprietary data or business-sensitive thresholds are exposed.

---

## Purpose

These models are designed to:

- Standardize raw outbound dialer events into a clean, queryable structure  
- Apply rule logic for business hours, redial behavior, and contact frequency  
- Produce daily compliance summaries suitable for BI dashboards  
- Demonstrate production-style SQL for analytics / data engineering roles  

---

## Repository Structure

### 1. `outbound_dials_view.sql`

Creates a standardized view over raw outbound dialer data:

- Normalizes timestamps and filenames  
- Exposes campaign, queue, media type, agent, and disposition fields  
- Includes dial counts, session-level metrics, ACW time, and time-to-abandon  
- Serves as the base “mart” layer for downstream compliance logic  

**Output:** one row per outbound dialing session.

---

### 2. `outbound_dnc_flags_view.sql`

Builds a compliance/enrichment layer on top of the standardized dials:

- Joins dialer activity to a customer/contact dimension  
- Derives contact-level features:  
  - Daily call sequence per customer  
  - 7-day rolling call count  
  - State code and threshold bucket  
- Calculates rule flags, for example:  
  - `business_hours_pass` – within allowable calling window  
  - `no_redial_after_answer_pass` – no additional attempts after a transfer/answer  
  - `state_daily_limit_pass` – daily contact frequency limit by state group  
  - `state_7day_limit_pass` – rolling contact limit by state group  
  - `other_state_limit_pass` – generic limit for all other states  

**Output:** one row per outbound dialing event with derived compliance flags.

---

### 3. `outbound_dnc_daily_summary.sql`

Aggregates the flag view to a daily, contact-level summary for BI tools:

- Groups by `interaction_date`  
- Counts distinct customers contacted per day  
- Counts distinct customers failing each rule type (business hours, redial, state limits)  
- Breaks out customer counts by anonymized state groups  

This result set is intended to be used as the dataset behind a dashboard such as  
an “Outbound Dialing Compliance by Date” report.

---

## Data Model Flow

The models follow a simple warehouse-style flow:

Raw Dialer Logs
↓
outbound_dials_view (standardized session-level data)
↓
outbound_dnc_flags_view (per-call compliance flags and enrichment)
↓
outbound_dnc_daily_summary (daily, contact-level compliance summary)


This pattern demonstrates how to evolve raw operational data into a
business-facing compliance view.

---

## Technology

All queries are written using **SQL for a cloud data warehouse** (Snowflake-style), using:

- CTEs (`WITH` clauses)  
- Window functions for row numbering and rolling 7-day counts  
- Conditional expressions for rule flags  
- Date/time transformations and formatting  
- Warehouse-style naming (`analytics.source_*`, `analytics.mart.*`, `analytics.dim.*`)  

---

## Notes

- All schemas, table names, state codes, and identifiers are anonymized.  
- No customer, employee, or actual regulatory thresholds are disclosed.  
- The logic is illustrative and intended to show SQL and modeling capability,
  not to represent a complete legal/compliance implementation.


