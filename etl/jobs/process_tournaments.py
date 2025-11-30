import json
from datetime import datetime, timezone

import etl.api.osirion_client as osr
import etl.db.loader as loader

from etl.db.models import (
    EventWindow,
    Match,
    MatchPlayer,
    DamageDealtEvent,
    EliminationEvent,
    get_session, reinit_db, get_engine
)
from etl.db.loader import (
    load_event_window_metadata,
    load_match_metadata,
    load_match_players,
    load_damage_dealt_events,
    load_elimination_events
)
from etl.parsing.event_parser import parse_event_window
from etl.parsing.match_parser import (
    parse_match_metadata, 
    parse_match_players,
    parse_elims, 
    parse_damage_dealt, 
)


def process_match(match_id: str, event_window_id: str, skip_if_exists: bool = False):
    session = get_session()

    # First process the match itself
    try:
        if skip_if_exists:
            existing = session.query(Match).filter_by(match_id=match_id).first()
            if existing:
                print(f"⏭️  Match {match_id} already processed, skipping...")
                return True
        print(f"\n{'='*60}")
        print(f"Processing match: {match_id}")
        print(f"{'='*60}\n")
        
        match_data = parse_match_metadata(match_id)

        if event_window_id:
            match_data["event_window_id"] = event_window_id

        players = parse_match_players(match_id)
        damage_dealt = parse_damage_dealt(match_id)
        elims = parse_elims(match_id)

        # Load into database (all in one transaction)
        print("\n💾 Loading into database...")
        loader.load_match_metadata(match_data, session)
        loader.load_match_players(players, match_id, session)
        loader.load_damage_dealt_events(damage_dealt, match_id, session)
        loader.load_elimination_events(elims, match_id, session)
        
        print(f"\n✅ Successfully processed match {match_id}\n")
        return True

    except Exception as e:
        print(f"\n❌ Error processing match {match_id}: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
        return False

    finally:
        session.close()



def process_event_window(event_window_id: str):
    """
    Process an entire event window
    """
    session = get_session()

    try:
        print(f"\n{'#'*60}")
        print(f"Processing Event Window: {event_window_id}")
        print(f"{'#'*60}\n")

        # fetch from API and load into data/raw
        event_window_data_path = osr.fetch_event_window_data(event_window_id)
        event_window_data = parse_event_window(event_window_id)

        event_window: EventWindow | None = session.query(EventWindow).filter_by(
            event_window_id=event_window_id
        ).first()
        if not event_window:
            loader.load_event_window_metadata(event_window_data, session)
            session.commit()
            event_window = session.query(EventWindow).filter_by(
                event_window_id=event_window_id
            ).first()
            if not event_window:
                raise RuntimeError(f"Failed to create event window {event_window_id}")

        if event_window.processed:
            print(f"⏭️  Event window already processed")
            return {"status": "already_processed"}

        event_window.processing = True
        event_window.last_processing_start = datetime.now(timezone.utc)


        # Load the eventWindow data into the DB first
        loader.load_event_window_metadata(event_window_data, session)

        # Fetch API data to get matches
        matches_path = osr.fetch_by_event_window(event_window_id)
        with open(matches_path, "r") as f:
            matches = json.load(f)["info"]

        # For each match parse
        for i, match in enumerate(matches):
            match_id = match["matchId"]

            process_match(match_id, event_window_id)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()  # Rollback on error
    finally:
        session.close() 


    if __name__ == "__main__":
        event_window_id = "S29_FNCS_Major2_GrandFinalDay2_EU"
        process_event_window(event_window_id)
