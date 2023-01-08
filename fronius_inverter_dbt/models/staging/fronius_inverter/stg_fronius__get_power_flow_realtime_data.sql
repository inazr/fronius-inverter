
SELECT
        Body_Data_Inverters_1_Battery_Mode,
        --SAFE_CAST(Body_Data_Inverters_1_DT AS INT64)        AS Body_Data_Inverters_1_DT,
        --Body_Data_Inverters_1_E_Day,
        SAFE_CAST(Body_Data_Inverters_1_E_Total AS NUMERIC) AS Body_Data_Inverters_1_E_Total,
        --Body_Data_Inverters_1_E_Year,
        SAFE_CAST(Body_Data_Inverters_1_P AS NUMERIC) AS Body_Data_Inverters_1_P,
        SAFE_CAST(Body_Data_Inverters_1_SOC AS NUMERIC) AS akku_charge_pct,
        SAFE_CAST(Body_Data_Site_BackupMode AS BOOL) AS Body_Data_Site_BackupMode,
        SAFE_CAST(Body_Data_Site_BatteryStandby AS BOOL) AS Body_Data_Site_BatteryStandby,
        --Body_Data_Site_E_Day,
        SAFE_CAST(Body_Data_Site_E_Total AS NUMERIC) AS Body_Data_Site_E_Total,
        --Body_Data_Site_E_Year,
        --Body_Data_Site_Meter_Location,
        --Body_Data_Site_Mode,
        -- positiv numbers indicate from where the electricity is flowing
        -- negativ numbers indicate to where the electricity is flowing
        SAFE_CAST(Body_Data_Site_P_Akku AS NUMERIC) AS akku_watt,
        SAFE_CAST(Body_Data_Site_P_Grid AS NUMERIC) AS grid_watt,
        SAFE_CAST(Body_Data_Site_P_Load AS NUMERIC) AS load_watt,
        SAFE_CAST(Body_Data_Site_P_PV AS NUMERIC) AS pv_watt,
        SAFE_CAST(Body_Data_Site_rel_Autonomy AS NUMERIC) AS autonomy_pct,
        SAFE_CAST(Body_Data_Site_rel_SelfConsumption AS NUMERIC) AS selfconsumption_pct,
        Body_Data_Version,
        --Head_Status_Code,
        --Head_Status_Reason,
        --Head_Status_UserMessage,
        Head_Timestamp
FROM
        {{ source('fronius_inverter', 'raw_get_power_from_realtime_data') }}