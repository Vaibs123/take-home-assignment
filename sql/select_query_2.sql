-- 2. for any given year, what type of crime (primary_type) was the most frequently committed?

with crime_counts as (
    select
        year,
        primary_type,
        count(primary_type) as counts
    from
        crime
    group by
        1,2
),
get_max_crime_count_per_year as 
(
    select
        year,
        max(counts) as max_crime_count
    from
        crime_counts
    group by
        year
)
select
    c.year,
    c.primary_type as crime,
    my.max_crime_count
from
    crime_counts as c
    inner join get_max_crime_count_per_year as my
        on my.year = c.year
        and my.max_crime_count = c.counts 
order by
    c.year
;