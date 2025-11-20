import json
import numpy as np
import pandas as pd
from collections import deque

from get_team_players import get_team_players
from get_match_players import get_id_to_name_map
from event_finder import get_hit_events

class TeamEngagement:
    def __init__(self, action):
        self.actions = [action]
        self.teams = {}

    def add(self, action):
        self.actions.append(action)

    def prev_action(self):
        return self.actions[-1]

    def time_recent(self):
        return self.actions[-1].get("timestamp")

def player_to_team(match_id):
    player_name_map = get_id_to_name_map(match_id)
    player_to_team = {}
    for id, nick in player_name_map.items():
        team_players = get_team_players(id, match_id)
        player_to_team["id"] = []
        for p in team_players:
            player_to_team["id"].append(p)

def check_connectivity(te: TeamEngagement, action, player_to_teams):
    time_diff = abs(te.prev_action["timestamp"] - action["timestamp"])
    hit_loc = action["location"]
    x2, y2, z2 = hit_loc["x"], hit_loc["y"], hit_loc["z"]

def group_damage_events(match_id: str):
    team_map = player_to_team(match_id)
    shot_actions = get_shot_events()

    events = deque()
    for shot in shot_actions:
        for e : events:
            # if most recent action in event was long ago, event has expired
            if e.time_recent() > THRESHOLD:
                events.popleft()
                continue
            # potential check for connectivity
            if check_connectivity(e, hit):
                e.add(hit)
        

if __name__ == "__main__":
    match_id = "832ceecc424df110d58e3e96d3dff834"
    groups = group_events(match_id)
