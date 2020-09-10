select
  id, full_name, team_name, goals, goals + assists as points, time_on_ice
from {{ ref('player_game_stats') }}
