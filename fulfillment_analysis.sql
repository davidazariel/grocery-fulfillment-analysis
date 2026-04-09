CREATE OR REPLACE TABLE `portfolio_project.data_mart.vendor_sla_comparison` AS
SELECT
  order_id,
  MAX(grocery_id) AS vendor_id,
  MAX(payment_timestamp) AS payment_timestamp,
  MAX(verification_timestamp) AS verification_timestamp,
  MAX(packing_start_timestamp) AS packing_start_timestamp,
  MAX(ready_for_pickup_timestamp) AS ready_for_pickup_timestamp,
  MAX(driver_arrived_timestamp) AS driver_arrived_timestamp,
  MAX(delivered_timestamp) AS delivered_timestamp,
  MAX(TIMESTAMP_DIFF(delivered_timestamp, payment_timestamp, MINUTE)) AS total_minutes,
  MAX(etd_max_date) AS ts_etd,
  MAX(logistics_option_name) AS logistics_option_name,
  MAX(logistics_service_name) AS logistics_service_name,
  SUM(COALESCE(total_product_tpv, 0)) AS total_order_tpv
FROM `rome-prod.datamart.order_info`
WHERE 1=1
  AND DATE(order_date) >= '2026-01-01'
  AND LOWER(status) = 'delivered'
  AND LOWER(logistics_option_name) IN ('2 jam sampai', 'instant')
  AND LOWER(grocery_id) IN ('grocery-a', 'grocery-b')
GROUP BY 1;

------------------------------------------------------------------------

SELECT
  CASE
    WHEN total_minutes <= 30 THEN '0-30m'
    WHEN total_minutes <= 45 THEN '31-45m'
    WHEN total_minutes <= 60 THEN '46-60m'
    WHEN total_minutes <= 120 THEN '61-120m'
    ELSE 'Over 120m'
  END AS fulfillment_time_bucket,
  COUNT(order_id) AS total_orders
FROM `portfolio_project.data_mart.vendor_sla_comparison`
GROUP BY 1
ORDER BY
  CASE fulfillment_time_bucket
    WHEN '0-30m' THEN 1
    WHEN '31-45m' THEN 2
    WHEN '46-60m' THEN 3
    WHEN '61-120m' THEN 4
    ELSE 5
  END;

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
  APPROX_QUANTILES(TIMESTAMP_DIFF(delivered_timestamp, payment_timestamp, MINUTE), 100)[OFFSET(75)] AS p75_total_cycle_time,
  APPROX_QUANTILES(TIMESTAMP_DIFF(delivered_timestamp, driver_arrived_timestamp, MINUTE), 100)[OFFSET(75)] AS p75_delivery_to_customer,
  APPROX_QUANTILES(TIMESTAMP_DIFF(driver_arrived_timestamp, ready_for_pickup_timestamp, MINUTE), 100)[OFFSET(75)] AS p75_waiting_for_driver,
  APPROX_QUANTILES(TIMESTAMP_DIFF(ready_for_pickup_timestamp, packing_start_timestamp, MINUTE), 100)[OFFSET(75)] AS p75_trigger_booking,
  APPROX_QUANTILES(TIMESTAMP_DIFF(packing_start_timestamp, verification_timestamp, MINUTE), 100)[OFFSET(75)] AS p75_merchant_process_order,
  APPROX_QUANTILES(TIMESTAMP_DIFF(verification_timestamp, payment_timestamp, MINUTE), 100)[OFFSET(75)] AS p75_system_verification
FROM `portfolio_project.data_mart.vendor_sla_comparison`
GROUP BY vendor_id;

------------------------------------------------------------------------

SELECT
  vendor_id,
  APPROX_QUANTILES(TIMESTAMP_DIFF(delivered_timestamp, payment_timestamp, MINUTE), 100)[OFFSET(90)] AS p90_total_cycle_time,
  APPROX_QUANTILES(TIMESTAMP_DIFF(delivered_timestamp, driver_arrived_timestamp, MINUTE), 100)[OFFSET(90)] AS p90_delivery_to_customer,
  APPROX_QUANTILES(TIMESTAMP_DIFF(driver_arrived_timestamp, ready_for_pickup_timestamp, MINUTE), 100)[OFFSET(90)] AS p90_waiting_for_driver,
  APPROX_QUANTILES(TIMESTAMP_DIFF(ready_for_pickup_timestamp, packing_start_timestamp, MINUTE), 100)[OFFSET(90)] AS p90_trigger_booking,
  APPROX_QUANTILES(TIMESTAMP_DIFF(packing_start_timestamp, verification_timestamp, MINUTE), 100)[OFFSET(90)] AS p90_merchant_process_order,
  APPROX_QUANTILES(TIMESTAMP_DIFF(verification_timestamp, payment_timestamp, MINUTE), 100)[OFFSET(90)] AS p90_system_verification
FROM `portfolio_project.data_mart.vendor_sla_comparison`
GROUP BY vendor_id;

------------------------------------------------------------------------

SELECT
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT CASE WHEN delivered_timestamp <= ts_etd THEN order_id END) AS total_ontime_orders,
  COUNT(DISTINCT CASE WHEN delivered_timestamp > ts_etd THEN order_id END) AS total_late_orders,
  COUNT(DISTINCT CASE WHEN delivered_timestamp > ts_etd THEN order_id END) * 10000 AS total_late_voucher_cost,
  SUM(total_order_tpv) AS total_tpv,
  SUM(total_order_tpv) - (COUNT(DISTINCT CASE WHEN delivered_timestamp > ts_etd THEN order_id END) * 10000) AS tpv_minus_voucher_cost,
  COUNT(DISTINCT CASE WHEN delivered_timestamp > ts_etd AND TIMESTAMP_DIFF(delivered_timestamp, ts_etd, MINUTE) <= 30 THEN order_id END) AS late_0_30m_past_etd,
  COUNT(DISTINCT CASE WHEN delivered_timestamp > ts_etd AND TIMESTAMP_DIFF(delivered_timestamp, ts_etd, MINUTE) > 30 AND TIMESTAMP_DIFF(delivered_timestamp, ts_etd, MINUTE) <= 60 THEN order_id END) AS late_31_60m_past_etd,
  COUNT(DISTINCT CASE WHEN delivered_timestamp > ts_etd AND TIMESTAMP_DIFF(delivered_timestamp, ts_etd, MINUTE) > 60 AND TIMESTAMP_DIFF(delivered_timestamp, ts_etd, MINUTE) <= 120 THEN order_id END) AS late_61_120m_past_etd,
  COUNT(DISTINCT CASE WHEN delivered_timestamp > ts_etd AND TIMESTAMP_DIFF(delivered_timestamp, ts_etd, MINUTE) > 120 THEN order_id END) AS late_over_120m_past_etd
FROM `portfolio_project.data_mart.vendor_sla_comparison`;

------------------------------------------------------------------------

SELECT
  logistics_service_name,
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT CASE WHEN delivered_timestamp <= ts_etd THEN order_id END) AS total_ontime_orders,
  COUNT(DISTINCT CASE WHEN delivered_timestamp > ts_etd THEN order_id END) AS total_late_orders,
  ROUND(COUNT(DISTINCT CASE WHEN delivered_timestamp > ts_etd THEN order_id END) / COUNT(DISTINCT order_id) * 100, 2) AS percentage_late_orders
FROM `portfolio_project.data_mart.vendor_sla_comparison`
GROUP BY 1
ORDER BY total_orders DESC;
