SELECT
        MAX(fct_fronius__app_data_daily.reporting_date) AS reporting_date,
        SUM(CASE WHEN fct_fronius__app_data_daily.reporting_date = CURRENT_DATE THEN Produktion END) AS Produktion_today,
        SUM(CASE WHEN fct_fronius__app_data_daily.reporting_date >= DATE_TRUNC(CURRENT_DATE, MONTH) THEN Produktion END) AS Produktion_month,
        SUM(CASE WHEN fct_fronius__app_data_daily.reporting_date >= DATE_TRUNC(CURRENT_DATE, YEAR) THEN Produktion END) AS Produktion_year,
        SUM(Produktion) AS Produktion_alltime

FROM
        {{ ref('fct_fronius__app_data_daily') }}