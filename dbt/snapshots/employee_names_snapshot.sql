{% snapshot employee_names_snapshot %}

{{
    config(
      target_database=target.database,
      target_schema='snapshots',
      strategy='check',
      unique_key='"employee_id"',
      check_cols=['"full_name"', '"job_id"'],
    )
}}

select * from {{ ref('employee_names') }}

{% endsnapshot %}