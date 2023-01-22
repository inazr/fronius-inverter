WITH charging AS (

    SELECT
            fct_fronius__app_data.reporting_date,
            percent,
            AVG(fct_fronius__app_data.in_die_Batterie) * COUNT(fct_fronius__app_data.in_die_Batterie) * 10 / 3600 AS kwh_in_die_batterie
    FROM
            UNNEST(GENERATE_ARRAY(1, 100)) AS percent
    LEFT JOIN
            --core.fct_fronius__app_data
            {{ ref('fct_fronius__app_data') }}

       ON   CAST(fct_fronius__app_data.Ladezustand AS INT64) = percent
      AND   fct_fronius__app_data.in_die_Batterie > 0

    GROUP BY
            1,2

)


,   discharging AS (

    SELECT
            fct_fronius__app_data.reporting_date,
            percent,
            AVG(fct_fronius__app_data.aus_der_Batterie) * COUNT(fct_fronius__app_data.aus_der_Batterie) * 10 / 3600 AS kwh_aus_der_Batterie
    FROM
            UNNEST(GENERATE_ARRAY(1, 100)) AS percent
    LEFT JOIN
            --core.fct_fronius__app_data
            {{ ref('fct_fronius__app_data') }}

       ON   CAST(fct_fronius__app_data.Ladezustand AS INT64) = percent
      AND   fct_fronius__app_data.aus_der_Batterie > 0

    GROUP BY
            1,2

)

SELECT
        COALESCE(charging.reporting_date, discharging.reporting_date) AS reporting_date,
        COALESCE(charging.percent, discharging.percent) AS percent,
        charging.kwh_in_die_batterie,
        discharging.kwh_aus_der_Batterie
FROM
        charging

FULL OUTER JOIN
        discharging
   ON   charging.reporting_date = discharging.reporting_date
  AND   charging.percent = discharging.percent