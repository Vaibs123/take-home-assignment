-- 5. For any beat, district, ward or community (only one will be provided at a time), 
-- retrieve all the unique keys of each crime incident ordered by date.

select
    unique_key,
    primary_type,
    date,
    beat,
    district,
    ward,
    community_area
from
    crime
-- where
    -- beat = <enter value here>
    -- district = <enter value here>
    -- ward = <enter value here>
    -- community_area = <enter value here>
order by
    date
;