SELECT#
        MAX(fct_fronius__app_data_daily.reporting_date) AS latest_reporting_date,
        AVG(fct_fronius__app_data_daily.Produktion) * 365 AS total_avg_production_per_year,
        AVG(fct_fronius__app_data_daily.Produktion) * 365 / 385 / 24 * 1000 AS total_avg_production_per_year_per_kilowatt_installed
FROM
        {{ ref('fct_fronius__app_data_daily') }}
        --`fronius-inverter`.core.fct_fronius__app_data_daily
WHERE
        fct_fronius__app_data_daily.reporting_date >= '2023-01-05'
