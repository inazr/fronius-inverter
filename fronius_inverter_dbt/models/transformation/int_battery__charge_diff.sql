SELECT
        Head_Timestamp,
        -- Returns the difference between the Head_Timestamp and the FOLLOWING value
        -- charging -> +
        -- discharging -> -
        LAG(akku_charge_pct, 1) OVER (ORDER BY Head_Timestamp DESC) - akku_charge_pct AS akku_charge_diff,
FROM
        {{ ref('stg_fronius__get_power_flow_realtime_data') }}
        --stg_fronius_inverter.stg_fronius__get_power_flow_realtime_data
ORDER BY
        Head_Timestamp DESC