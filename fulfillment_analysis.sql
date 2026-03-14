CREATE OR REPLACE TABLE `portfolio_project.data_mart.vendor_sla_comparison` AS
SELECT
  order_id,
  MAX(grocery_id) AS vendor_id,
  MAX(payment_timestamp) AS payment_timestamp,
  MAX(verification_timestamp) AS verification_timestamp,
  MAX(packing_start_timestamp) AS packing_start_timestamp,
  MAX(ready_for_pickup_timestamp) AS ready_for_pickup_timestamp,
  MAX(driver_arrived_timestamp) AS driver_arrived_timestamp,
  MAX(delivered_timestamp) AS delivered_timestamp
FROM `portfolio_project.raw_data.daily_transactions`
WHERE 1=1
  AND order_date >= '2026-01-01'
  AND order_status = 'Delivered'
  AND grocery_id IN ('GROCERY-A', 'GROCERY-B')
GROUP BY order_id;

------------------------------------------------------------------------

SELECT
  vendor_id,
  AVG(TIMESTAMP_DIFF(delivered_timestamp, payment_timestamp, MINUTE)) AS avg_total_cycle_time,
  AVG(TIMESTAMP_DIFF(delivered_timestamp, driver_arrived_timestamp, MINUTE)) AS avg_delivery_to_customer,
  AVG(TIMESTAMP_DIFF(driver_arrived_timestamp, ready_for_pickup_timestamp, MINUTE)) AS avg_waiting_for_driver,
  AVG(TIMESTAMP_DIFF(ready_for_pickup_timestamp, packing_start_timestamp, MINUTE)) AS avg_trigger_booking,
  AVG(TIMESTAMP_DIFF(packing_start_timestamp, verification_timestamp, MINUTE)) AS avg_merchant_process_order,
  AVG(TIMESTAMP_DIFF(verification_timestamp, payment_timestamp, MINUTE)) AS avg_system_verification
FROM `portfolio_project.data_mart.vendor_sla_comparison`
GROUP BY
  vendor_id;

------------------------------------------------------------------------

SELECT
  vendor_id,
  APPROX_QUANTILES(TIMESTAMP_DIFF(delivered_timestamp, payment_timestamp, MINUTE), 100)[OFFSET(50)] AS median_total_cycle_time,
  APPROX_QUANTILES(TIMESTAMP_DIFF(delivered_timestamp, payment_timestamp, MINUTE), 100)[OFFSET(75)] AS p75_total_cycle_time,
  APPROX_QUANTILES(TIMESTAMP_DIFF(delivered_timestamp, driver_arrived_timestamp, MINUTE), 100)[OFFSET(75)] AS p75_delivery_to_customer,
  APPROX_QUANTILES(TIMESTAMP_DIFF(driver_arrived_timestamp, ready_for_pickup_timestamp, MINUTE), 100)[OFFSET(75)] AS p75_waiting_for_driver,
  APPROX_QUANTILES(TIMESTAMP_DIFF(ready_for_pickup_timestamp, packing_start_timestamp, MINUTE), 100)[OFFSET(75)] AS p75_trigger_booking,
  APPROX_QUANTILES(TIMESTAMP_DIFF(packing_start_timestamp, verification_timestamp, MINUTE), 100)[OFFSET(75)] AS p75_merchant_process_order,
  APPROX_QUANTILES(TIMESTAMP_DIFF(verification_timestamp, payment_timestamp, MINUTE), 100)[OFFSET(75)] AS p75_system_verification,
  APPROX_QUANTILES(TIMESTAMP_DIFF(delivered_timestamp, payment_timestamp, MINUTE), 100)[OFFSET(90)] AS p90_total_cycle_time
FROM `portfolio_project.data_mart.vendor_sla_comparison`
GROUP BY
  vendor_id;
