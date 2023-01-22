

    SELECT
            fct_fronius__app_data.reporting_date,
            'charging' AS type,
            percent,
            AVG(fct_fronius__app_data.in_die_Batterie) * COUNT(fct_fronius__app_data.in_die_Batterie) * 10 / 3600 AS kwh
    FROM
            UNNEST(GENERATE_ARRAY(1, 100)) AS percent
    LEFT JOIN
            --core.fct_fronius__app_data
            {{ ref('fct_fronius__app_data') }}

       ON   CAST(fct_fronius__app_data.Ladezustand AS INT64) = percent
      AND   fct_fronius__app_data.in_die_Batterie > 0

    GROUP BY
            1,2,3

UNION ALL

    SELECT
            fct_fronius__app_data.reporting_date,
            'discharging' AS type,
            percent,
            AVG(fct_fronius__app_data.aus_der_Batterie) * COUNT(fct_fronius__app_data.aus_der_Batterie) * 10 / 3600 AS kwh
    FROM
            UNNEST(GENERATE_ARRAY(1, 100)) AS percent
    LEFT JOIN
            --core.fct_fronius__app_data
            {{ ref('fct_fronius__app_data') }}

       ON   CAST(fct_fronius__app_data.Ladezustand AS INT64) = percent
      AND   fct_fronius__app_data.aus_der_Batterie > 0

    GROUP BY
            1,2,3