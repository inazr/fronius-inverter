SELECT
        fct_fronius__app_data_daily.reporting_date,
        CAST(fct_fronius__app_data_daily.Eigenverbrauch * (0.2399 - (0.2399 / 1.19)) * 0.19 * -1 AS NUMERIC) AS Eigenverbrauchabgabe, -- (Brutto Hausstrom - Netto Hausstrom) * MwSt * -1
        fct_fronius__app_data_daily.Verbrauch * 0.2399 AS Opportunitaetserloes,
        fct_fronius__app_data_daily.Netzeinspeisung * 0.082 AS Einspeiseverguetung,    --Anlagen bis 10 kWp erhalten 8,2 Cent pro kWh.
FROM
        {{ ref('fct_fronius__app_data_daily') }}