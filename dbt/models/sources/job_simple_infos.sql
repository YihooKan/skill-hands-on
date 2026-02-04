select
    "job_id",
    "job_title"
from {{ source('skill_hands_on_source', 'jobs') }}