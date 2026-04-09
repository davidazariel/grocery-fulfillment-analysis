# E-Commerce Fulfillment SLA Analysis

This project investigates the end-to-end fulfillment performance by comparing two major grocery partners, Grocery A and Grocery B. Focusing exclusively on completed orders that utilized the 2-hour delivery option, this analysis evaluates the entire delivery lifecycle.

The primary objective is to evaluate their operational readiness for the upcoming 'Blitz Delivery' program. Unlike the current flow, Blitz Delivery automatically books a courier the moment an order arrives. By analyzing the fulfillment process, this project pinpoints bottlenecks, conducts stress tests for extreme scenarios, and assesses which grocery partner is truly ready for this new model.

In this showcase, I highlight how the raw data was transformed into a structured Data Mart using SQL, the key metrics extracted, and how the finalized data structure helps business users understand operational delays and their financial impacts.

## Key SQL Techniques Used
* **Data Materialization:** Makes a smaller, focused table from huge raw data to optimize dashboard loading speed and save server costs.
* **Targeted Filtering:** Isolates relevant data by strictly filtering for successfully delivered orders that specifically used the 2-hour delivery service.
* **Data Flattening:** Uses GROUP BY and MAX() aggregation to condense multiple status logs into a single, complete operational timeline per order.
* **Granular Bottleneck Tracking:** Uses TIMESTAMP_DIFF to measure the exact minute duration of every micro step in the fulfillment funnel.
* **Outlier Mitigation:** Computes the 75th Percentile (P75) and 90th Percentile (P90) using the APPROX_QUANTILES function to evaluate performance under extreme heavy load scenarios.
* **Financial Impact Simulation:** Calculates the total cost of delayed orders by simulating a 10k compensation voucher for every late delivery to understand the potential revenue leak.
