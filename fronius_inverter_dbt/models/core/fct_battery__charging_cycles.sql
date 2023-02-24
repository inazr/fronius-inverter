SELECT
        DATE(int_battery__charge_diff.Head_Timestamp) AS reporting_date,
        SUM(CASE WHEN int_battery__charge_diff.akku_charge_diff > 0 THEN int_battery__charge_diff.akku_charge_diff ELSE 0 END) AS percent_loading,
        SUM(CASE WHEN int_battery__charge_diff.akku_charge_diff < 0 THEN int_battery__charge_diff.akku_charge_diff ELSE 0 END) AS percent_unloading,
        SUM(CASE WHEN int_battery__charge_diff.akku_charge_diff > 0 THEN int_battery__charge_diff.akku_charge_diff ELSE 0 END) / 100 AS full_charging_cycle
FROM
        {{ ref('int_battery__charge_diff') }}
        --transformation.int_battery__charge_diff
GROUP BY
        1