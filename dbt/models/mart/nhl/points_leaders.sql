select 
team_name,
full_name,
max(points) as points
from {{ ref('nhl_players') }}  -- or other tables
group by full_name, team_name
order by points