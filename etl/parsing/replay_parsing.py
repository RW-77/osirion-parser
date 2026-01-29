import uuid
import json
import logging
import numpy as np

import boto3
from pathlib import Path
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

from etl.parsing.cleaning import get_id_to_name_map


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

    dt = 1.0 / hz

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
    movement_events = sorted(data["movement_events"], key=lambda x: x["timestamp"])

    state = {
        id: {
            "x": 0.0,
            "y": 0.0,
            "z": 0.0,
            "yaw": 0.0,
            "health": 100.0,
            "shield": 0.0,
            "alive": True,
            "dbno": False,
        } for id in player_map.keys()
    }

    frames = []
    t_0 = info["aircraftStartTime"]
    t_f = info["endTimestamp"]
    next_t = t_0

    for evt in all_events:
        evt_type = evt["type"]
        timestamp = evt["timestamp"]
        data = evt["data"]
        id = data["epicId"]

        while next_t <= evt["timestamp"]:
            # capture the current state in the return frames
            frames.
            next_t += dt

        # update the state based on the event
        match evt["type"]:
            case "movement":
                state[id]["x"] = data["location"]["x"]
                state[id]["y"] = data["location"]["y"]
                state[id]["z"] = data["location"]["z"]
                state[id]["yaw"] = data["rotationYaw"]
            case "knock":
                if state[id]["alive"] == False:
                    print(f"Knock on dead player {id} at t={timestamp}")
                    print(f"\t\"alive\" should be True but is False")
                    state[id]["alive"] = True
                state[id]["dbno"] = True
                if not state[id]["dead"] == True:
                    print(f"Knock on dead player {id} at t={timestamp}")
                    print(f"\t\"dead\" should be False but is True")
                    state[id]["dead"] = False
            case "elimination":
                state[id]["alive"] = False
                state[id]["dbno"] = False
                state[id]["health"] = 0.0
                state[id]["shield"] = 0.0
            case "health_update":
                if state[id]["alive"] == False:
                    print(f"Knock on dead player {id} at t={timestamp}")
                    print(f"\t\"alive\" should be True but is False")
                    state[id]["alive"] = True
                if not state[id]["dead"] == True:
                    print(f"Knock on dead player {id} at t={timestamp}")
                    print(f"\t\"dead\" should be False but is True")
                    state[id]["dead"] = False
                state[id]["health"] = data["value"]
            case "shield_update":
                if state[id]["alive"] == False:
                    print(f"Knock on dead player {id} at t={timestamp}")
                    print(f"\t\"alive\" should be True but is False")
                    state[id]["alive"] = True
                if not state[id]["dead"] == True:
                    print(f"Knock on dead player {id} at t={timestamp}")
                    print(f"\t\"dead\" should be False but is True")
                    state[id]["dead"] = False
                state[id]["shield"] = data["value"]
            case "revive":
                state[id]["alive"] = True
                state[id]["dbno"] = False
                # state[id]["dead"] = False
            case "reboot":
                state[id]["alive"] = True
                # state[id]["dbno"] = False
                state[id]["dead"] = False
            case _:
                print(f"Current event for player {id} does not have an event type.")
                raise Exception

    return frames


if __name__ == "__main__":
    match_id = "832ceecc424df110d58e3e96d3dff834"
    frames = get_match_object(match_id=match_id, hz=20)
