with employees as (
    select * from {{ ref('employee_names') }}
),

jobs as (
    select * from {{ ref('job_simple_infos') }}
)


select 
    e.employee_id,
    e.full_name,
    j.job_title
from employees e
left join jobs j
on e."job_id" = j."job_id"