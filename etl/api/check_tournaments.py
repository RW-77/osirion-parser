import os
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.osirion.gg/fortnite/v1"
API_KEY = os.getenv("API_KEY") 
INTERVAL_SECONDS = 2592000  # last 30 days
SEEN_FILE = "data/events/seen_tournaments.json"


def load_seen():
    if not os.path.exists(SEEN_FILE):
        return set()
    try:
        with open(SEEN_FILE, "r") as f:
            return set(json.load(f))
    except Exception:
        return set()


def save_seen(seen_set):
    Path(SEEN_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen_set), f, indent=2)


def fetch_tournaments():
    url = f"{BASE_URL}/tournaments?intervalS={INTERVAL_SECONDS}"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    print(json.dumps(data, indent=2))
    return data


def check_for_new_tournaments():
    seen = load_seen()
    tournaments = fetch_tournaments().get("tournaments")
    print(json.dumps(tournaments, indent=2))

    # Deduplicate by eventWindowId
    unique = {}
    for t in tournaments:
        if isinstance(t, dict):
            event_window = t.get("eventWindowId")
            if event_window and event_window not in unique:
                unique[event_window] = t

    new_tournaments = [t for eid, t in unique.items() if eid not in seen]

    if not new_tournaments:
        print("No new tournaments found in the past month.")
        return

    print(f"Found {len(new_tournaments)} new tournament(s) in the past month:")
    for t in new_tournaments:
        title = t.get("displayData", {}).get("title") or t.get("eventId")
        event_window = t.get("eventWindowId")
        print(f"• {title} ({event_window})")
        seen.add(event_window)

    save_seen(seen)


if __name__ == "__main__":
    try:
        check_for_new_tournaments()
    except Exception as e:
        print(f"Error: {e}")
