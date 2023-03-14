-- Save model as 'dbt_results.sql'

{{
  config(
    materialized = 'incremental',
    transient = False,
    unique_key = 'result_id'
  )
}}

with empty_table as (
    select
        CAST(null AS STRING) as result_id,
        CAST(null as STRING) AS invocation_id,
        CAST(null as STRING) AS unique_id,
        CAST(null as STRING) AS database_name,
        CAST(null as STRING) AS schema_name,
        CAST(null as STRING) AS name,
        CAST(null as STRING) AS resource_type,
        CAST(null as STRING) AS status,
        cast(null as NUMERIC) as execution_time,
        cast(null as INT64) as rows_affected
)

select * from empty_table
-- This is a filter so we will never actually insert these values
where 1 = 0