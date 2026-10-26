with raw_data as (
    select 
        *,
        -- Handle Duplicates: If the API returns the same complaint twice, assign a row number to pick the newest one
        row_number() over (partition by unique_key order by created_date desc) as rn
    from {{ source('bronze', 'raw_nyc_311') }}
    where unique_key is not null
),

deduplicated as (
    select * from raw_data where rn = 1
)

select
    cast(unique_key as string) as complaint_id,
    
    -- Safe casting prevents the pipeline from crashing if a date format suddenly changes
    safe_cast(created_date as timestamp) as created_at,
    safe_cast(closed_date as timestamp) as closed_at,
    
    -- Text cleaning: Remove trailing spaces and convert empty strings to true NULLs
    nullif(trim(agency), '') as agency,
    nullif(trim(agency_name), '') as agency_name,
    nullif(trim(complaint_type), '') as complaint_type,
    nullif(trim(descriptor), '') as descriptor,
    
    -- Geographic Edge Cases: Filter out default 0.0 coordinates or values outside NYC bounds
    case 
        when safe_cast(latitude as float64) between 40.4 and 41.0 then safe_cast(latitude as float64)
        else null 
    end as latitude,
    
    case 
        when safe_cast(longitude as float64) between -74.3 and -73.7 then safe_cast(longitude as float64)
        else null 
    end as longitude,
    
    -- ZIP Code Edge Cases: Ensure it is exactly 5 numeric digits, otherwise set to NULL
    case 
        when length(trim(incident_zip)) = 5 and regexp_contains(trim(incident_zip), r'^[0-9]+$') then trim(incident_zip)
        else null 
    end as zip_code,
    
    -- Standardization: Force all city names to uppercase so 'BROOKLYN' and 'Brooklyn' match in BI tools
    upper(nullif(trim(city), '')) as city,
    nullif(trim(status), '') as status

from deduplicated
-- Logical Edge Case: Ensure the ticket wasn't closed before it was even created
where safe_cast(created_date as timestamp) <= coalesce(safe_cast(closed_date as timestamp), current_timestamp())