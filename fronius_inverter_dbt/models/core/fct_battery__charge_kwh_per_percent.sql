SELECT
        reporting_date,
        percent,
        AVG(in_die_Batterie) * COUNT(in_die_Batterie) * 10 / 3600 AS kwh_in_die_batterie
FROM
        UNNEST(GENERATE_ARRAY(1, 100)) AS percent
LEFT JOIN
        --core.fct_fronius__app_data
        {{ ref('fct_fronius__app_data') }}

   ON   CAST(Ladezustand AS INT64) = percent
  AND   in_die_Batterie > 0

GROUP BY
        1,2