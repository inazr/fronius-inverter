-- Need to check calculations

SELECT
        DATE(Head_Timestamp, 'Europe/Berlin') AS reporting_date,
        -- Produktion
        AVG(pv_watt) * COUNT(DISTINCT EXTRACT(HOUR FROM DATETIME(Head_Timestamp, 'Europe/Berlin'))) / 1000 AS Produktion,
        (AVG(pv_watt) + AVG(CASE WHEN grid_watt < 0 THEN grid_watt ELSE 0 END)) * COUNT(DISTINCT EXTRACT(HOUR FROM DATETIME(Head_Timestamp, 'Europe/Berlin'))) / 1000 AS Eigenverbrauch,
        ((AVG(pv_watt) + AVG(CASE WHEN grid_watt < 0 THEN grid_watt ELSE 0 END)) + AVG(CASE WHEN akku_watt < 0 THEN akku_watt ELSE 0 END)) * COUNT(DISTINCT EXTRACT(HOUR FROM DATETIME(Head_Timestamp, 'Europe/Berlin'))) / 1000 AS Direktverbrauch,
        AVG(CASE WHEN akku_watt < 0 THEN akku_watt ELSE 0 END) * COUNT(DISTINCT EXTRACT(HOUR FROM DATETIME(Head_Timestamp, 'Europe/Berlin'))) / 1000 * -1 AS in_die_Batterie,
        AVG(CASE WHEN grid_watt < 0 THEN grid_watt ELSE 0 END)  * COUNT(DISTINCT EXTRACT(HOUR FROM DATETIME(Head_Timestamp, 'Europe/Berlin'))) / 1000 * -1 AS Netzeinspeisung,

        -- Verbrauch
        (AVG(pv_watt) + AVG(CASE WHEN grid_watt < 0 THEN grid_watt ELSE 0 END) + AVG(CASE WHEN akku_watt < 0 THEN akku_watt ELSE 0 END) + AVG(CASE WHEN akku_watt > 0 THEN akku_watt ELSE 0 END) + AVG(CASE WHEN grid_watt > 0 THEN grid_watt ELSE 0 END)) * COUNT(DISTINCT EXTRACT(HOUR FROM DATETIME(Head_Timestamp, 'Europe/Berlin'))) / 1000 AS Verbrauch,
        (AVG(pv_watt) + AVG(CASE WHEN grid_watt < 0 THEN grid_watt ELSE 0 END) + AVG(CASE WHEN akku_watt < 0 THEN akku_watt ELSE 0 END) + AVG(CASE WHEN akku_watt > 0 THEN akku_watt ELSE 0 END)) * COUNT(DISTINCT EXTRACT(HOUR FROM DATETIME(Head_Timestamp, 'Europe/Berlin'))) / 1000  AS Eigenerzeugung,
        AVG(CASE WHEN akku_watt > 0 THEN akku_watt ELSE 0 END) * COUNT(DISTINCT EXTRACT(HOUR FROM DATETIME(Head_Timestamp, 'Europe/Berlin'))) / 1000 AS aus_der_Batterie,
        AVG(CASE WHEN grid_watt > 0 THEN grid_watt ELSE 0 END)  * COUNT(DISTINCT EXTRACT(HOUR FROM DATETIME(Head_Timestamp, 'Europe/Berlin'))) / 1000 AS Netzbezug,

        -- Battery
        AVG(akku_charge_pct) AS Ladezustand

FROM
        {{ ref('stg_fronius__get_power_flow_realtime_data') }}
        --stg_fronius_inverter.stg_fronius__get_power_flow_realtime_data
GROUP BY
        1
ORDER BY
        1