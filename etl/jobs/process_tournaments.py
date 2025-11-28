import json
import etl.api.osirion_client as osr
import etl.db.loader as loader

from etl.parsing.match_parser import parse_elims, parse_damage_dealt


def process_match(match_id: str, skip_if_exists: bool = False):

    elims = parse_elims(match_id)
    damage_dealt = parse_damage_dealt(match_id)

    # call loading function

    # update the processed time of the match


def process_event_window(event_window_id: str):
    # Fetch API data to get matches
    matches_path = osr.fetch_by_event_window(event_window_id)
    with open(matches_path, "r") as f:
        matches = json.load(f)["info"]

    # For each match parse
    for i, match in enumerate(matches):
        match_id = match["matchId"]

        process_match(match_id)
