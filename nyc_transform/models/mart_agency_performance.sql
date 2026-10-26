select
    agency,
    agency_name,
    count(complaint_id) as total_complaints,
    countif(status = 'Closed') as closed_complaints,
    round(avg(timestamp_diff(closed_at, created_at, hour)), 2) as avg_resolution_hours
from {{ ref('stg_nyc_311') }}
group by 1, 2