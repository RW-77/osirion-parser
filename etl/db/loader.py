import json
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
from etl.models import get_session, Match, Player, DamageEvent, EliminationEvent, init_db
from etl.parsing.match_parser import parse_damage_dealt, parse_elims


def load_match_players(match_id: str, session):
    """
    Load all unique players from a match into the database.
    You'll need to get this from the match info or player list.
    """
    # TODO: Implement based on API player list endpoint
    # collect from events for now
    pass


def load_damage_events(match_id: str, match_start_timestamp: int, session):
    """
    Parse and load damage events for a match.
    """
    print(f"Parsing damage events for match {match_id}...")
    damage_events = parse_damage_dealt(match_id)
    
    print(f"Loading {len(damage_events)} damage events into database...")
    
    # Collect unique player IDs
    player_ids = set()
    
    for event in damage_events:
        player_ids.add(event["actor_id"])
        player_ids.add(event["recipient_id"])
    
    # Ensure players exist (create if needed)
    for player_id in player_ids:
        existing = session.query(Player).filter_by(
            epicId=player_id,
            matchId=match_id
        ).first()
        
        if not existing:
            player = Player(
                epicId=player_id,
                epicUsername=player_id,  # Replace with actual lookup
                matchId=match_id
            )
            session.add(player)
    
    session.commit()
    
    # Bulk insert damage events
    damage_records = []
    for event in damage_events:
        game_time_seconds = (event["timestamp"] - match_start_timestamp) // 1000
        
        damage_records.append({
            "matchId": match_id,
            "timestamp": datetime.fromtimestamp(event["timestamp"] / 1000),
            "gameTimeSeconds": game_time_seconds,
            "shooterId": event["actor_id"],
            "victimId": event["recipient_id"],
            "weaponId": event["weapon_id"],
            "damageAmount": 0,  # You'll need to add this to your parser
            "shooterX": event["ax"],
            "shooterY": event["ay"],
            "shooterZ": event["az"],
            "victimX": event["rx"],
            "victimY": event["ry"],
            "victimZ": event["rz"],
            "distance": event["distance"],
            "zone": event["zone"],
        })
    
    session.bulk_insert_mappings(DamageEvent, damage_records)
    session.commit()
    
    print(f"✅ Loaded {len(damage_records)} damage events")


def load_elimination_events(match_id: str, match_start_timestamp: int, session):
    """
    Parse and load elimination events for a match.
    """
    print(f"Parsing elimination events for match {match_id}...")
    elim_events = parse_elims(match_id)
    
    print(f"Loading {len(elim_events)} elimination events into database...")
    
    # Collect unique player IDs
    player_ids = set()
    for event in elim_events:
        player_ids.add(event["actor_id"])
        player_ids.add(event["recipient_id"])
    
    # Ensure players exist
    for player_id in player_ids:
        existing = session.query(Player).filter_by(
            epicId=player_id,
            matchId=match_id
        ).first()
        
        if not existing:
            player = Player(
                epicId=player_id,
                epicUsername=player_id,
                matchId=match_id
            )
            session.add(player)
    
    session.commit()
    
    # Bulk insert elimination events
    elim_records = []
    for event in elim_events:
        game_time_seconds = (event["timestamp"] - match_start_timestamp) // 1000
        
        elim_records.append({
            "matchId": match_id,
            "timestamp": datetime.fromtimestamp(event["timestamp"] / 1000),
            "gameTimeSeconds": game_time_seconds,
            "actorId": event["actor_id"],
            "victimId": event["recipient_id"],
            "weaponId": event["weapon_id"],
            "actorX": event["ax"],
            "actorY": event["ay"],
            "actorZ": event["az"],
            "victimX": event["rx"],
            "victimY": event["ry"],
            "victimZ": event["rz"],
            "distance": event["distance"],
            "zone": event["zone"],
        })
    
    session.bulk_insert_mappings(EliminationEvent, elim_records)
    session.commit()
    
    print(f"✅ Loaded {len(elim_records)} elimination events")


def load_match(match_id: str, match_start_timestamp: int):
    """
    Load all data for a match into the database.
    """
    session = get_session()
    
    try:
        # Check if match already exists
        existing_match = session.query(Match).filter_by(id=match_id).first()
        
        if existing_match:
            print(f"⚠️  Match {match_id} already exists. Skipping...")
            return
        
        # Create match record
        match = Match(
            id=match_id,
            startTime=datetime.fromtimestamp(match_start_timestamp / 1000)
        )
        session.add(match)
        session.commit()
        print(f"✅ Created match record for {match_id}")
        
        # Load all event types
        load_damage_events(match_id, match_start_timestamp, session)
        load_elimination_events(match_id, match_start_timestamp, session)
        
        print(f"✅ Successfully loaded all data for match {match_id}")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Error loading match {match_id}: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    # Initialize database tables (run once)
    # init_db()
    
    # Load match
    match_id = "832ceecc424df110d58e3e96d3dff834"
    
    # Need to get the match start timestamp from match_info.json
    import json
    with open(f"data/raw/match_{match_id}/match_info.json") as f:
        match_info = json.load(f)
        match_start = match_info["startTimestamp"]
    
    load_match(match_id, match_start)
