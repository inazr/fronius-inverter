SELECT
        DATETIME_TRUNC(Head_Timestamp, MINUTE, 'Europe/Berlin') AS reporting_minute,
        DATE(Head_Timestamp, 'Europe/Berlin') AS reporting_date,
        EXTRACT(TIME FROM Head_Timestamp) AS reporting_time,

        -- Produktion
        AVG(pv_watt) / 1000 AS Produktion,
        (AVG(pv_watt) + AVG(CASE WHEN grid_watt < 0 THEN grid_watt ELSE 0 END)) / 1000 AS Eigenverbrauch,
        ((AVG(pv_watt) + AVG(CASE WHEN grid_watt < 0 THEN grid_watt ELSE 0 END)) + AVG(CASE WHEN akku_watt < 0 THEN akku_watt ELSE 0 END)) / 1000 AS Direktverbrauch,
        AVG(CASE WHEN akku_watt < 0 THEN akku_watt ELSE 0 END) / 1000 * -1 AS in_die_Batterie,
        AVG(CASE WHEN grid_watt < 0 THEN grid_watt ELSE 0 END) / 1000 * -1 AS Netzeinspeisung,

        -- Verbrauch
        (AVG(pv_watt) + AVG(CASE WHEN grid_watt < 0 THEN grid_watt ELSE 0 END) + AVG(CASE WHEN akku_watt < 0 THEN akku_watt ELSE 0 END) + AVG(CASE WHEN akku_watt > 0 THEN akku_watt ELSE 0 END) + AVG(CASE WHEN grid_watt > 0 THEN grid_watt ELSE 0 END)) / 1000 AS Verbrauch,
        (AVG(pv_watt) + AVG(CASE WHEN grid_watt < 0 THEN grid_watt ELSE 0 END) + AVG(CASE WHEN akku_watt < 0 THEN akku_watt ELSE 0 END) + AVG(CASE WHEN akku_watt > 0 THEN akku_watt ELSE 0 END)) / 1000  AS Eigenerzeugung,
        AVG(CASE WHEN akku_watt > 0 THEN akku_watt ELSE 0 END) / 1000 AS aus_der_Batterie,
        AVG(CASE WHEN grid_watt > 0 THEN grid_watt ELSE 0 END) / 1000 AS Netzbezug,

        -- Battery
        AVG(akku_charge_pct) AS Ladezustand

FROM
        {{ ref('stg_fronius__get_power_flow_realtime_data') }}
        --stg_fronius_inverter.stg_fronius__get_power_flow_realtime_data
GROUP BY
        1,2,3
ORDER BY
        1

