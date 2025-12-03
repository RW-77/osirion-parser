import os
from pathlib import Path


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


def match_fetched(match_id, data_dir: str = "data/raw"):
    base_path = Path(data_dir) / f"match_{match_id}"
        
    # Define required files for a match
    required_files = {
        'info': base_path / "info.json",
        'events': base_path / "events.json",
        'movement_events': base_path / "movement_events.json",
        'shot_events': base_path / "shot_events.json"
    }
    
    # Check existence of each file
    result = {
        file_type: file_path.exists() 
        for file_type, file_path in required_files.items()
    }
    
    # Add summary key
    result['all_exist'] = all(result.values())
    
    return result


def get_missing_match(match_id, data_dir: str = "data/raw"):
    pass


if __name__ == "__main__":
    event_id_good = "S29_FNCS_Major2_GrandFinalDay2_EU"
    print(event_window_fetched(event_id_good))
