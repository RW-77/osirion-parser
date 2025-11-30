import os
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.osirion.gg/fortnite/v1"
API_KEY = os.getenv("API_KEY") 
HEADERS = {"Authorization": f"Bearer {API_KEY}"}


def _make_request(url: str, params: dict | None = None) -> dict:
    print(f"API key: {API_KEY}")
    res = requests.get(url, headers=HEADERS, params=params or {})
    if res.status_code != 200:
        raise RuntimeError(f"Error {res.status_code}: {res.text[:200]}")
    return res.json()


def _save_json(data: dict, path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Saved to {path}")


def session_to_match_id(session_id: str) -> str | None:
    """
    Returns the match ID for a given session ID.
    """
    url = f"{BASE_URL}/matches/session-id-to-match-id?serverRecordedOnly=true&sessionIds={session_id}"
    params = {}
    data = _make_request(url, params)

    match_id = data.get("matchIds", {}).get(session_id)
    if match_id:
        print(f"Session ID: {session_id}")
        print(f"Match ID: {match_id}")
        return match_id
    else:
        print("No match ID found in response.")
        print(json.dumps(data, indent=2))
        return None


def get_id_to_name_map(match_id: str) -> dict | None:
    """
    Returns a mapping of player IDs to Epic usernames for all players for 
    given match.
    """

    url = f"{BASE_URL}/matches/{match_id}/players"
    params = {}

    data = _make_request(url, params)

    players = data.get("players")
    if players:
        map = {
            p["epicId"]: p["epicUsername"] 
            for p in players
            if not (
                p["isSpectator"]
                or p["isBot"]
                or p["epicUsername"].startswith("BLAST_")
                or p["epicUsername"].startswith("OBS_")
            )
        }
        return map

    else:
        print("No players found in response.")
        print(json.dumps(data, indent=2))
        return {}


def get_team_players(epic_id: str, match_id: str):
    url = f"{BASE_URL}/matches/{match_id}/team?epicId={epic_id}"
    params = {}

    data = _make_request(url, params)

    player_data: list[dict] = data.get("players", {})
    team_players = [p["epicId"] for p in player_data]
    if team_players:
        print(f"Found {len(team_players)} players")
        return team_players
    else:
        print("Could not find team players.")
        print(json.dumps(data, indent=2))
        return None


def fetch_match_info(match_id: str, out_dir="data/raw") -> str:
    url = f"{BASE_URL}/matches/{match_id}"
    data = _make_request(url)
    out_path =f"{out_dir}/match_{match_id}/info.json"
    _save_json(data, out_path)
    return out_path


def fetch_match_events(match_id: str, out_dir="data/raw") -> str:
    url = f"{BASE_URL}/matches/{match_id}/events"

    params = {
        "include": (
            "safeZoneUpdateEvents,"
            "reviveEvents, rebootEvents, knockedDownEvents, eliminationEvents,"
            "playerInventoryUpdateEvents,"
            "landingEvents"
        ) 
    }
    data = _make_request(url, params)
    out_path = f"{out_dir}/match_{match_id}/match_events.json"
    _save_json(data, out_path)
    return out_path


def fetch_match_movement_events(match_id: str, 
                              out_dir="data/raw", 
                              start_time=0,
                              end_time=1650) -> str:
    url = f"{BASE_URL}/matches/{match_id}/events/movement"
    params = { "startTimeRelative": {start_time}, "endTimeRelative": {end_time} }
    data = _make_request(url, params)
    out_path = f"{out_dir}/match_{match_id}/match_movement_events.json"
    _save_json(data, out_path)
    return out_path


def fetch_match_shot_events(match_id: str, 
                              out_dir="data/raw", 
                              start_time=0,
                              end_time=1650) -> str:
    url = f"{BASE_URL}/matches/{match_id}/events/shots"
    params = {"startTimeRelative": start_time, "endTimeRelative": end_time}
    data = _make_request(url, params)
    out_path = f"{out_dir}/match_{match_id}/match_shot_events.json"
    _save_json(data, out_path)
    return out_path


def fetch_event_window_data(event_window_id: str, out_dir="data/raw"):
    url = f"{BASE_URL}/tournaments"
    params = { "eventWindowId": event_window_id }
    data = _make_request(url, params)
    out_path = f"{out_dir}/event_window_{event_window_id}/info.json"
    _save_json(data, out_path)
    return out_path


def fetch_by_event_window(event_window_id: str, out_dir="data/raw"):
    url = f"{BASE_URL}/matches"
    params = { "eventWindowId": event_window_id, "ignoreUploads": True }
    data = _make_request(url, params)
    out_path = f"{out_dir}/event_window_{event_window_id}/event_window_matches.json"
    _save_json(data, out_path)
    return out_path


def fetch_by_event(event_id: str, out_dir="data/raw"):
    url = f"{BASE_URL}/matches"
    params = { "eventId": event_id, "ignoreUploads": True }
    data = _make_request(url, params)
    out_path = f"{out_dir}/event_window_{event_id}/event_matches.json"
    _save_json(data, out_path)
    return out_path


if __name__ == "__main__":
    season = 37
    # match_id = "832ceecc424df110d58e3e96d3dff834"
    # fetch_match_info(match_id)
    event_window = "S29_FNCS_Major2_GrandFinalDay2_EU"
    ew_data_path = fetch_event_window_data(event_window)
