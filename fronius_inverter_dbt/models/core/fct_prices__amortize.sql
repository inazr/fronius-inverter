WITH aggregates AS (

    SELECT
            MAX(fct_prices__electricity.reporting_date) AS latest_reporting_date,
            AVG(fct_prices__electricity.Eigenverbrauchabgabe)
          + AVG(fct_prices__electricity.Opportunitaetserloes)
          + AVG(fct_prices__electricity.Einspeiseverguetung) AS avg_income_per_day,
            SUM(DISTINCT seed_price_onetime.price_netto) AS total_costs,
    FROM
            {{ ref('fct_prices__electricity') }}
--             core.fct_prices__electricity
    CROSS JOIN
            {{ ref('seed_price_onetime') }}
--             seeds.seed_price_onetime

)

SELECT
        latest_reporting_date,
        avg_income_per_day,
        total_costs,
        total_costs / avg_income_per_day AS num_of_days_to_amortize,
        total_costs / avg_income_per_day / 365 AS num_of_years_to_amortize,
FROM
        aggregates