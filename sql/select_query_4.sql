-- 4. What is the frequency of each crime type (primary_type) year over year? 
-- Imagine we want to generate datapoints to graph the number of occurrences of each crime type over every year in the dataset.

select
    primary_type as crime,
    year,
    count(primary_type) as counts
from
    crime
group by
    1,2
order by
    crime,
    year
;
