import os
import json
import pandas as pd
import numpy as np

from bisect import bisect_left
from collections import defaultdict


def calculate_distances(coords_pairs):
    """Calculate 3D distances from coordinate pairs."""
    a_arr = np.array([c[0] for c in coords_pairs], dtype=np.float32)
    r_arr = np.array([c[1] for c in coords_pairs], dtype=np.float32)
    diff = a_arr - r_arr
    return np.sqrt(np.sum(diff**2, axis=1))

def get_time_table(player_events, reference_events):
    """
    Returns for each player, the event belonging to them in `player_events` 
    occuring closest in time to each event in `reference_events`.
    Assumes `reference_events` is already sorted by `timestamp`.
    """
    target_ts = np.sort(np.array([e["timestamp"] for e in reference_events]), kind="stable")
    sorted_per_player = defaultdict(list)

    for e in player_events:
        sorted_per_player[e["epicId"]].append(e)
    for player_id in sorted_per_player:
        sorted_per_player[player_id].sort(key=lambda e: e["timestamp"])

    cache = defaultdict(dict)
    for player_id, events in sorted_per_player.items():
        event_ts = np.array([me["timestamp"] for me in events])
        indices = np.searchsorted(event_ts, target_ts)
        indices = np.clip(indices, 1, len(event_ts)-1)

        before = event_ts[indices - 1]
        after = event_ts[indices]

        # pick the closer shot (in time)
        choose_after = np.abs(after - target_ts) < np.abs(before - target_ts)
        closest_idxs = np.where(choose_after, indices, indices - 1)

        # Extract the movement events
        closest_player_events = [events[i] for i in closest_idxs]

        # Store results
        cache[player_id]["timestamps"] = event_ts
        cache[player_id]["closest_indices"] = closest_idxs
        cache[player_id]["closest_events"] = closest_player_events

    return cache


def build_zone_timeline(zone_events: list[dict]):
    assert len(zone_events) == 12, f"Expected 12 zone events, got {len(zone_events)}"
    
    zone_timeline = []
    zone_events.sort(key=lambda e: e["currentPhase"])
    
    for i, zone_event in enumerate(zone_events):
        end_time = zone_event["shrinkEndTime"]
        zone_timeline.append(end_time)
    return zone_timeline


def parse_elims(match_id):
    """
    Time
    Distance
    Weapon
    """
    match_path = f"data/raw/match_{match_id}"

    with (
        open(f"{match_path}/shot_events.json", "r") as f1,
        open(f"{match_path}/match_info.json", "r") as f2,
        open(f"{match_path}/eliminationEvents.json", "r") as f3,
        open(f"{match_path}/safeZoneUpdateEvents.json", "r") as f4
    ):
        shot_events = json.load(f1)["hitscanEvents"]
        match_info = json.load(f2)
        elim_events = json.load(f3)
        zone_events = json.load(f4)


    match_start = match_info["startTimestamp"]
    zone_timeline = build_zone_timeline(zone_events)

    elim_events.sort(key=lambda e: e["timestamp"])

    enriched_elims = []
    coord_pairs = []

    for i, ee in enumerate(elim_events):
        # self eliminations do not show actor location
        if ee["selfElimination"]:
            continue

        actor_id = ee["epicId"]
        recipient_id = ee["targetId"]

        ts = ee["timestamp"]
        zone = bisect_left(zone_timeline, ts) + 1
        weapon_id = ee["gunType"]

        actor_loc = ee["playerLocation"]
        recipient_loc = ee["targetLocation"]

        coord_pairs.append((
            (actor_loc["x"], actor_loc["y"], actor_loc["z"]),
            (recipient_loc["x"], recipient_loc["y"], recipient_loc["z"])
        ))

        enriched_elims.append({
            "timestamp": ts,
            "zone": zone,
            "weapon_id": weapon_id,
            "actor_id": actor_id,
            "recipient_id": recipient_id,
            "ax": actor_loc["x"],
            "ay": actor_loc["y"],
            "az": actor_loc["z"],
            "rx": recipient_loc["x"],
            "ry": recipient_loc["y"],
            "rz": recipient_loc["z"],
        })

    distances = calculate_distances(coord_pairs)
    for i, event in enumerate(enriched_elims):
        event["distance"] = float(distances[i])

    return enriched_elims


def parse_damage(match_id):
    """
    Time
    Distance
    Weapon
    """
    match_path = f"data/raw/match_{match_id}"

    shot_events_path = f"{match_path}/shot_events.json"
    movement_events_path = f"{match_path}/movement_events.json"
    general_events_path = f"{match_path}/events.json"
    match_info_path = f"{match_path}/match_info.json"

    with (
        open(general_events_path, "r") as f1,
        open(movement_events_path, "r") as f2,
        open(shot_events_path, "r") as f3,
        open(match_info_path, "r") as f4,
    ):
        general_events = json.load(f1)
        movement_events = json.load(f2)["events"]
        shot_events = json.load(f3)["hitscanEvents"]
        match_info = json.load(f4)

    zone_events = general_events["safeZoneUpdateEvents"]
    health_update_events = general_events["healthUpdateEvents"]
    shield_update_events = general_events["shieldUpdateEvents"]
    revive_events = general_events["reviveEvents"]
    reboot_events = general_events["rebootEvents"]

    match_start = match_info["startTimestamp"]

    zone_timeline = build_zone_timeline(zone_events)
    hit_events = [e for e in shot_events if e.get("hitPlayer")]
    hit_events.sort(key=lambda e: e["timestamp"])

    pos_cache = get_time_table(movement_events, hit_events)

    enriched_damages = []

    coord_pairs = []
    for i, he in enumerate(hit_events):
        ts = he["timestamp"]
        zone = bisect_left(zone_timeline, ts) + 1

        weapon_id = he["weaponId"]

        actor_id = he["epicId"]
        actor_move_event = pos_cache[actor_id]["closest_events"][i]
        actor_loc = actor_move_event["movementData"]["location"]

        recipient_id = he["hitEpicId"]
        recipient_loc = he["location"]

        coord_pairs.append((
            (actor_loc["x"], actor_loc["y"], actor_loc["z"]),
            (recipient_loc["x"], recipient_loc["y"], recipient_loc["z"])
        ))

        enriched_damages.append({
            "timestamp": ts,
            "zone": zone,
            "weapon_id": weapon_id,
            "actor_id": actor_id,
            "recipient_id": recipient_id,
            "ax": actor_loc["x"],
            "ay": actor_loc["y"],
            "az": actor_loc["z"],
            "rx": recipient_loc["x"],
            "ry": recipient_loc["y"],
            "rz": recipient_loc["z"],
        })

    distances = calculate_distances(coord_pairs)
    for i, event in enumerate(enriched_damages):
        event["distance"] = float(distances[i])

    return enriched_damages


if __name__ == "__main__":
    match_id = "832ceecc424df110d58e3e96d3dff834"
    # damage_events = parse_damage(match_id)
    # print(json.dumps(damage_events, indent=2))
    elim_events = parse_elims(match_id)
    print(json.dumps(elim_events, indent=2))
