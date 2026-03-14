# E-Commerce Fulfillment SLA Analysis

This project investigates the end-to-end fulfillment performance by comparing two major grocery partners (Grocery A vs. Grocery B). The primary objective is to evaluate the entire delivery lifecycle from the moment a customer completes a payment until the order is successfully delivered. 

In this showcase, I highlight how the raw data was transformed into a structured Data Mart using SQL, the key metrics extracted, and how the finalized data structure helps business users pinpoint operational bottlenecks.

## Key SQL Techniques Used
* **Data Materialization:** Makes a smaller, focused table from huge raw data to optimize dashboard loading speed and save server costs.
* **Targeted Filtering:** Isolates relevant data by strictly filtering for successfully 'Delivered' orders from specific vendors starting from January 2026 onwards.
* **Data Flattening:** Uses GROUP BY and MAX() aggregation to condense multiple status logs into a single, complete operational timeline per order.
* **Granular Bottleneck Tracking:** Uses TIMESTAMP_DIFF to measure the exact minute duration of every micro step in the fulfillment funnel.
* **Outlier Mitigation:** Computes the Median (P50), 75th Percentile (P75), and 90th Percentile (P90) utilizing the APPROX_QUANTILES function to eliminate the impact of extreme data skewness.
