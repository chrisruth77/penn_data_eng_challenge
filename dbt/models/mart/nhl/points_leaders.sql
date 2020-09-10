select 
team_name, full_name, max(points) as max_points 
from {{ ref('nhl_players') }}
group by team_name 
