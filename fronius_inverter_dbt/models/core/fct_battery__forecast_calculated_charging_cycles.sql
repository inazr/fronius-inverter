SELECT
        SUM(CASE WHEN fct_battery__charging_cycles.reporting_date_cest >= CURRENT_DATE - INTERVAL 1 YEAR THEN fct_battery__charging_cycles.full_charging_cycle END)
      / COUNT(CASE WHEN fct_battery__charging_cycles.reporting_date_cest >= CURRENT_DATE - INTERVAL 1 YEAR THEN fct_battery__charging_cycles.full_charging_cycle END)
      * 365 AS total_charging_cycles_per_year_based_last_365_days,
        SUM(fct_battery__charging_cycles.full_charging_cycle)
      / COUNT(fct_battery__charging_cycles.full_charging_cycle)
      * 365 AS total_charging_cycles_per_year_based_all_time,

FROM
        {{ ref('fct_battery__charging_cycles') }}
