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


def match_movement_object(match_id: str, hz: int):
    """
    Parses the movement_events.json log of a match and returns a binary object
    containing all player positions at regular intervals.
    """

    sample_dt = 1.0 / hz

    match_path = Path("data/raw/match_{match_id}")

    match_logs = [ 
        "movement_events",
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
    print(f"  - {sum(1 for e in all_events if e['type'] == 'damage')} damage events")
    print(f"  - {sum(1 for e in all_events if e['type'] == 'elimination')} elimination events")
    print(f"  - {sum(1 for e in all_events if e['type'] == 'health_update')} health updates")
    print(f"  - {sum(1 for e in all_events if e['type'] == 'shield_update')} shield updates")

    player_map: dict = get_id_to_name_map(match_id)
    movement_events = sorted(data["movement_events"], key=lambda x: x["timestamp"])
    samples = []
    start_time = info["aircraftStartTime"]
    end_time = info["endTimestamp"]

    state = {
        id: {
            "alive": True,
            "x": None,
            "y": None,
            "z": None
        } for id in player_map.keys()
    }
    ret = {}
    t = start_time
    i = 0

    while t <= end_time:
        while i < len(movement_events) and movement_events[i]["timestamp"] <= t:
            ev = movement_events[i]
            ret[ev["player_id"]] = (
                ev["x"], ev["y"], ev["z"], ev["yaw"]
            )
            i += 1
