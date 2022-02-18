{{ config(materialized='table') }}

-- more efficient to be 'table' since it will be exposed to BI tools

select 
    locationid, 
    borough, 
    zone, 
    replace(service_zone,'Boro','Green') as service_zone
from {{ ref('taxi_zone_lookup') }}