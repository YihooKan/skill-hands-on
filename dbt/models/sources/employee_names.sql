{# {{config(materialized='table')}} #}
-- use jinja syntax to comment out if needed {#{{...}}#}
-- top most priority configuration
-- will override dbt_project.yml config

{{
    config(
        materialized='incremental',
        unique_key='employee_id'
    )
}}
-- notice: if change to incremental, it will be generated as table,
-- even if in dbt_project.yml it is set as view

select
    "employee_id",
    concat("first_name", ' ', "last_name") as "full_name",
    "job_id"
from {{ source('skill_hands_on_source', 'employees') }}

-- NOTE: 
-- WHAT: if materialized as incremental, and build or test this sql will fail
-- WHY: because in the marco it will try to get the actual table from db,
-- but in test env, the table does not exist yet.
-- HOW: so need to override the is_incremental marco to always return false in the unit test.
-- REFERENCE: see dbt/models/sources/employee_names.yml line 21-25
-- GitHub issue reference: https://github.com/dbt-labs/dbt-core/issues/9593

{% if is_incremental()%}
    where "employee_id" not in (select "employee_id" from {{this}})
{% endif %}