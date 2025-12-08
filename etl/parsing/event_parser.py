import json
from datetime import datetime

from etl.api.osirion_client import fetch_by_event_window


def parse_event_window_metadata(event_window_id):
    event_window_path = f"data/raw/event_window_{event_window_id}"
    try:
        with (
            open(f"{event_window_path}/info.json", "r") as f1,
            open(f"{event_window_path}/matches.json") as f2
        ):
            event_window_info = json.load(f1)
            event_window_matches = json.load(f2)["matches"]
    except FileNotFoundError:
        raise ValueError(f"Event window files not found.")

    event_window_matches.sort(key=lambda e: e["startTimestamp"])
    total_matches = len(event_window_matches)

    first_match = event_window_matches[1]
    last_match = event_window_matches[-1]

    start_time = first_match.get("startTimestamp")
    if start_time is not None:
        start_time = datetime.fromtimestamp(start_time / 1e6)
    end_time = last_match.get("endTimestamp")
    if end_time is not None:
        end_time = datetime.fromtimestamp(end_time / 1e6)

    return {
        "event_window_id": event_window_id,
        "start_time": start_time,
        "end_time": end_time,
        "total_matches": total_matches
    }

def parse_event_matches(event_window_id):
    event_window_matches_path = f"data/raw/event_window_{event_window_id}/matches.json"
    with open(event_window_matches_path, "r") as f:
        matches = json.load(f)["matches"]

        print(f"Found {len(matches)} matches to process\n")

    return matches
