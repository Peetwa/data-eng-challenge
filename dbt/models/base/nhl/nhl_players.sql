select
  nhl_player_id as id,
  full_name,
  game_team_name as team_name,
  stats_assists as assists,
  stats_goals as goals,
  stats_goals + stats_assists as points
from {{ ref('player_game_stats') }} --or whatever other table reference
where
nhl_player_id is not null and
full_name is not null and
game_team_name is not null and
stats_assists is not null and
stats_goals is not null

