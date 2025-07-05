# 4-3_Work_with_ClickHouse_mv
## ClickHouse: Event Aggregation and 7-Day Retention Analysis
📁 Project Overview

### This project implements:
* Storage of raw user events (user_events table)
* Creation of an aggregated daily table (user_events_daily_agg) for trend analysis
* A Materialized View that automatically updates aggregates on data insert
* Calculation of 7-day user retention
* A query for daily analytics by event type and date

### Table Structure
user_events (Raw Logs)
| event_date  | event_type | unique_users  | total_spent | total_actions |
| ------------- |:-------------:| :-------------:| :-------------:| :-------------:|
| 2025-03-20      | login    | 2 | 0  | 2  
| 2025-03-20      | signup   | 1 | 0  | 1  
| 2025-03-23      | login    | 2 | 0  | 2 
| 2025-03-27      | purchase | 1 | 50 | 1 
| 2025-03-28      | login    | 1 | 0  | 1 
| 2025-03-20      | signup   | 1 | 0  | 1  

### Aggregate Functions Used
* uniqState(user_id) / uniqMerge() — Count of unique users
* sumState(points_spent) / sumMerge() — Total points spent
* countState() / countMerge() — Total actions

### 7-Day Retention Calculation
The retention query returns:
* total_users_day_0: signup date
* returned_in_7_days: number of users who returned within 7 days
* retention_7d_percent: percentage of retained users

### Daily Analytics Query
* Query to analyze activity by event_type and event_date using merge functions:

### ✅ Completion Checklist
 * Two tables and a Materialized View created
 * Uses sumState, uniqState, and countState
 * Corresponding merge functions implemented
 * Retention logic implemented and queried
 * Daily aggregation query written

