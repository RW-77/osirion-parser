import uuid
import copy
import json
import logging
import numpy as np

import boto3
from pathlib import Path
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

from etl.parsing.cleaning import get_id_to_name_map
from etl.api.preprocessing import get_players


class ObjectWrapper:
    """Encapsulates S3 object actions."""

    def __init__(self, s3_object):
        """
        Params:
            s3_object: A Boto3 Object resource.
        """
        self.object = s3_object
        self.key = self.object.key

    def get(self):
        """
        Returns the object data in bytes.
        """
        try:
            body = self.object.get()["Body"].read()
            print()
        except:
            raise

    def delete(self):
        """
        Deletes the object.
        """
        try:
            self.object.delete()
            self.object.wait_until_not_exists()
            print(
                "Deleted object '%s' from bucket '%s'.",
                self.object.key,
                self.object.bucket_name,
            )
        except ClientError:
            print(
                "Couldn't delete object '%s' from bucket '%s'.",
                self.object.key,
                self.object.bucket_name,
            )
            raise


def get_match_object(match_id: str, hz: int):
    """
    Parses the movement_events.json log of a match and returns a binary object
    containing all player positions at regular intervals.
    """

    match_path = Path(f"data/raw/match_{match_id}")

    match_logs = [ 
        "movement_events",
        "knockedDownEvents",
        "eliminationEvents",
        "healthUpdateEvents",
        "shieldUpdateEvents",
        "reviveEvents",
        "rebootEvents",
    ]
    info = json.loads((match_path / "info.json").read_text())
    data = {
        name: json.loads((match_path / f"{name}.json").read_text())
        for name in match_logs
    }

    all_events = []

    for event in data["movement_events"]:
        all_events.append({
            "type": "movement",
            "timestamp": event["timestamp"],
            "data": event
        })
    for event in data["eliminationEvents"]:
        all_events.append({
            "type": "elimination",
            "timestamp": event["timestamp"],
            "data": event
        })
    for event in data["healthUpdateEvents"]:
        all_events.append({
            "type": "health_update",
            "timestamp": event["timestamp"],
            "data": event
        })
    for event in data["shieldUpdateEvents"]:
        all_events.append({
            "type": "shield_update",
            "timestamp": event["timestamp"],
            "data": event
        })
    for event in data["reviveEvents"]:
        all_events.append({
            "type": "revive",
            "timestamp": event["timestamp"],
            "data": event
        })
    for event in data["rebootEvents"]:
        all_events.append({
            "type": "reboot",
            "timestamp": event["timestamp"],
            "data": event
        })

    all_events.sort(key=lambda x: x["timestamp"])

    print(f"Merged {len(all_events)} total events:")
    print(f"  - {sum(1 for e in all_events if e['type'] == 'movement')} movement events")
    print(f"  - {sum(1 for e in all_events if e['type'] == 'elimination')} elimination events")
    print(f"  - {sum(1 for e in all_events if e['type'] == 'health_update')} health updates")
    print(f"  - {sum(1 for e in all_events if e['type'] == 'shield_update')} shield updates")
    print(f"  - {sum(1 for e in all_events if e['type'] == 'revive')} revive updates")
    print(f"  - {sum(1 for e in all_events if e['type'] == 'reboot')} reboot updates")

    player_map: dict = get_id_to_name_map(match_id)

    player_ids = list(player_map.keys())
    player_index = {pid: i for i, pid in enumerate(player_ids)}
    N = len(player_ids)

    state = np.zeros((N,8), dtype=np.float32)
    state[:, 4] = 100.0 # alive
    state[:, 6] = 1.0 # alive

    frames = []
    t_0 = info["aircraftStartTime"]  # microseconds
    next_t = 0.0  # seconds (relative to aircraftStartTime)
    dt = 1.0 / hz  # seconds

    for evt in all_events:
        evt["timestamp"] = (evt["timestamp"] - t_0) * 1e-6 # seconds

    bot_players = get_players(match_id, is_bot=True)
    for k, v in bot_players.items():
        print(k)

    for i, evt in enumerate(all_events):

        evt_type = evt["type"]
        timestamp = evt["timestamp"]
        data = evt["data"]

        id = data["epicId"]
        idx = player_index.get(id)

        if data["epicId"] in bot_players.keys():
            continue

        # for all frames between this event (state) and the previous
        # copy the state to the frame
        while next_t <= evt["timestamp"]:
            # capture the current state in the return frames
            frames.append(state.copy())
            next_t += dt

        target_id = data.get("targetId")
        target_idx = player_index.get(target_id)
            
        # update the state based on the event
        # 1: x
        # 2: y
        # 3: z
        # 4: hp
        # 5: shield
        # 6: alive
        # 7: knocked/dbno
        match evt["type"]:
            case "movement":
                state[idx, 0] = data["movementData"]["location"]["x"]
                state[idx, 1] = data["movementData"]["location"]["y"]
                state[idx, 2] = data["movementData"]["location"]["z"]
                state[idx, 3] = data["movementData"]["rotationYaw"]
            case "knock":
                # NOTE: players can have shield and health when knocked
                state[target_idx, 6] = 1.0
                state[target_idx, 7] = 1.0
            case "elimination":
                state[target_idx, 6] = 0.0
                state[target_idx, 7] = 0.0
                state[target_idx, 4] = 0.0
                state[target_idx, 5] = 0.0
            case "health_update":
                state[idx, 4] = data["value"]
            case "shield_update":
                state[idx, 5] = data["value"]
            case "revive":
                state[idx, 6] = True
                state[idx, 7] = False
            case "reboot":
                state[idx, 6] = True
                state[idx, 7] = False
            case _:
                print(f"Current event for player {id} does not have an event type.")
                raise Exception

    return player_index, frames



if __name__ == "__main__":
    match_id = "832ceecc424df110d58e3e96d3dff834"
    frames = get_match_object(match_id=match_id, hz=20)
    print(frames)
