-- 3. What percentage of each type of crime (primary_type) ended in arrest (arrest == true)?
select
    primary_type,
    (count(case when arrest then 1 end)::float /  count(*)) * 100 as percent_arrest_each_crime
from
    crime
group by
    primary_type;
