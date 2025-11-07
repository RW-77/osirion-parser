import os
import json
import requests
from session_to_match_id import session_to_match_id


if __name__ == "__main__":
    match_id = "832ceecc424df110d58e3e96d3dff834"
    epic_id = "37da09eb8b574968ad36da5adc02232b"
    team_players = get_team_players(epic_id, match_id)
    print(json.dumps(team_players, indent=2))
