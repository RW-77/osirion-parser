import os

from pathlib import Path
from typing import Callable

import etl.api.osirion_client as osr


EVENT_TYPES = {
    "info": osr.fetch_match_info,
    "players": osr.fetch_match_players,
    "movement_events": osr.fetch_match_movement_events,
    "shot_events": osr.fetch_match_shot_events,
    "safeZoneUpdateEvents": osr.fetch_match_events,
    "reviveEvents": osr.fetch_match_events,
    "rebootEvents": osr.fetch_match_events
}


def event_window_fetched(event_window_id, data_dir: str = "data/raw"):
    base_path = Path(data_dir) / f"event_window_{event_window_id}"
    
    # Define required files for an event window
    required_files = {
        'info': base_path / "info.json",
        'matches': base_path / "matches.json"
    }
    
    # Check existence of each file
    result = {
        file_type: file_path.exists() 
        for file_type, file_path in required_files.items()
    }
    
    # Add summary key
    result['all_exist'] = all(result.values())
    
    return result


def fetch_match_missing(
    match_id: str, 
    out_dir: str= "data/raw", 
    event_types: dict[str, Callable] | None = None
) -> dict:

    if event_types is None:
        event_types = EVENT_TYPES

    print(f"Fetching all missing data for match {match_id}...")

    match_dir = Path(out_dir) / f"match_{match_id}"
    
    missing = []
    for event_type in event_types.keys():
        filepath = match_dir / f"{event_type}.json"
        if not filepath.exists():
            missing.append(event_type)

    if not missing:
        print(f"All files exist for match {match_id}")
        return {
            "all_exist": True,
            "fetched": []
        }

    called = set()
    fetched = []

    for event_type in missing:
        fn = event_types[event_type]
        if fn not in called:
            print(f"\tCalling {fn.__name__} for {event_type}")
            fn(match_id, out_dir)
            called.add(fn)
            fetched.append(event_type)

    return {
        "all_exist": True,
        "fetched": fetched
    }


def fetch_match_all(
    match_id: str, 
    out_dir: str = "data/raw", 
    event_types: dict | None = None
) -> dict:
    """
    Fetch all data for a single match from Osirion API.
    """
    if event_types is None:
        event_types = EVENT_TYPES

    print(f"Fetching all data for match {match_id}...")

    paths = {}
    called = set()

    try:
        for event_type, fn in event_types.items():
            if fn not in called:
                print(f"\tCalling {fn.__name__} for {event_type}")
                paths[event_type] = fn(match_id, out_dir)
                called.add(fn)

        print(f"✅ Successfully fetched all data for match {match_id}")
        return paths
        
    except Exception as e:
        print(f"❌ Error fetching data for match {match_id}: {e}")
        raise


if __name__ == "__main__":
    event_id_good = "S29_FNCS_Major2_GrandFinalDay2_EU"
    print(event_window_fetched(event_id_good))
