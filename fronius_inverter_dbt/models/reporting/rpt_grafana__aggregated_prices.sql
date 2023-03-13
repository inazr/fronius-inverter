SELECT
        MAX(fct_prices__electricity.reporting_date) AS reporting_date,
        SUM(CASE WHEN fct_prices__electricity.reporting_date = CURRENT_DATE THEN Bilanz END) AS Bilanz_today,
        SUM(CASE WHEN fct_prices__electricity.reporting_date >= DATE_TRUNC(CURRENT_DATE, MONTH) THEN Bilanz END) AS Bilanz_month,
        SUM(CASE WHEN fct_prices__electricity.reporting_date >= DATE_TRUNC(CURRENT_DATE, YEAR) THEN Bilanz END) AS Bilanz_year,
        SUM(Bilanz) AS Bilanz_alltime

FROM
        {{ ref('fct_prices__electricity') }}