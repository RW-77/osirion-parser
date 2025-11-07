import os
import json
import time
from collections import defaultdict

from pipeline.api import osirion_client as osr

def parse_elims(players_path: str, shot_events_path: str, opts=None):
    pass

def parse_damage(players_path: str, shot_events_path: str, opts=None):
    """
    Returns a dict of all player damage
    """
    with open(players_path, "r") as f:
        player_data = json.load(f)
    with open(shot_events_path, "r") as f:
        shot_data = json.load(f)

    player_ids = {
        p["epicId"]: p["epicUsername"]
        for p in player_data["players"]
        if not (
            p["isSpectator"]
            or p["isBot"]
        )
    }
    player_dmg = defaultdict(lambda: {"dmg": 0})
    shot_events = shot_data["hitscanEvents"]
    for se in shot_events:
        if se["hitPlayer"]:
            player_dmg[player_ids.get(se["epicId"], "unknown")]["dmg"] += se["damage"]

    print(json.dumps(player_dmg, indent=2))
