SELECT
        reporting_date,
        percent,
        AVG(aus_der_Batterie) * COUNT(aus_der_Batterie) * 10 / 3600 AS kwh_aus_der_Batterie
FROM
        UNNEST(GENERATE_ARRAY(1, 100)) AS percent
LEFT JOIN
        --core.fct_fronius__app_data
        {{ ref('fct_fronius__app_data') }}

   ON   CAST(Ladezustand AS INT64) = percent
  AND   aus_der_Batterie > 0

GROUP BY
        1,2