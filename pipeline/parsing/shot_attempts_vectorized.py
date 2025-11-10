import json
import cProfile
import math
import pandas as pd
import numpy as np
from glob import glob
from collections import defaultdict
from bisect import bisect_left, bisect_right
import matplotlib.pyplot as plt

from pipeline.api import osirion_client as osr
from geometry.vec3 import Vec3, normalize, dot
from geometry.ray import Ray
from geometry.sphere import Sphere


def get_hit_attempt_events(shot_events_path: str, movement_events_path: str):
    """
    Returns a list of all shots events which are attempts to hit exposed players
    """

    with open(movement_events_path, "r") as f:
        movement_events = json.load(f)["events"]
    with open(shot_events_path, "r") as f:
        shot_events = json.load(f)["hitscanEvents"]

    with open("data/processed/test_teammate_map.json", "r") as f:
        team_player_ids = json.load(f)
        print(f"Found {len(team_player_ids)} in match.")

    # target timestamps
    target_ts = np.sort(np.array([se["timestamp"] for se in shot_events]), kind="stable")

    sorted_movements_by_player = defaultdict(list)
    for me in movement_events:
        sorted_movements_by_player[me["epicId"]].append(me)
    for player_id in sorted_movements_by_player:
        sorted_movements_by_player[player_id].sort(key=lambda e: e["timestamp"])

    res = defaultdict(dict)
    for player, movement_events in sorted_movements_by_player.items():
        movement_ts = np.array([me["timestamp"] for me in movement_events])
        indices = np.searchsorted(movement_ts, target_ts)
        indices = np.clip(indices, 1, len(movement_ts)-1)

        before = movement_ts[indices - 1]
        after = movement_ts[indices]
        # Choose whichever side is closer for each shot
        choose_after = np.abs(after - target_ts) < np.abs(before - target_ts)
        closest_idxs = np.where(choose_after, indices, indices - 1)

        # Extract the movement events
        closest_movements = [movement_events[i] for i in closest_idxs]

        # Store results
        res[player_id]["timestamps"] = movement_ts
        res[player_id]["closest_indices"] = closest_idxs
        res[player_id]["closest_events"] = closest_movements

    '''
    print("\n=== Time Differences Per Shot Per Player ===")
    for shot_idx, ts in enumerate(target_ts):
        diffs = {}
        for player_id, pdata in res.items():
            me = pdata["closest_events"][shot_idx]
            diffs[player_id] = abs(me["timestamp"] - ts)
        # Sort players by smallest difference (optional)
        sorted_diffs = dict(sorted(diffs.items(), key=lambda x: x[1]))
        print(f"Shot {shot_idx:04d} (ts={ts}):")
        for pid, dt in list(sorted_diffs.items())[:5]:  # show top 5 closest players
            print(f"  {pid}: {dt:.0f}")
    '''


    player_ids = list(res.keys())
    num_shots = len(target_ts)
    num_players = len(player_ids)

    diff_matrix = np.zeros((num_shots, num_players), dtype=np.float64)
    for j, player_id in enumerate(player_ids):
        pdata = res[player_id]
        for i in range(num_shots):
            diff_matrix[i, j] = abs(pdata["closest_events"][i]["timestamp"] - target_ts[i])

    diff_matrix /= 1000.0
# --- Visualization ---
    plt.figure(figsize=(12, 6))
    plt.imshow(diff_matrix, aspect='auto', cmap='viridis', interpolation='nearest')
    plt.colorbar(label='Time Difference (ms)')
    plt.xlabel("Player index")
    plt.ylabel("Shot index")
    plt.title("Time difference between each shot and each player's closest movement event")

    plt.tight_layout()
    plt.show()


















if __name__ == '__main__':
    movement_events_path = "data/raw/match_movement_events.json"
    shot_events_path = "data/raw/match_shot_events.json"
    # attempts = get_hit_attempt_events(shot_events_path, movement_events_path)
    res = get_hit_attempt_events(shot_events_path, movement_events_path)
    print(res)

    # cProfile.run('print(get_hit_attempt_events(shot_events_path, movement_events_path))', sort='tottime')
