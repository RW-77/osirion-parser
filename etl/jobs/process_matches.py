from pathlib import Path
import numpy as np

from etl.parsing.replay_parsing import get_match_object
from etl.storage.s3 import ObjectWrapper
from etl.transform.chunking import chunk_frames


def process_match_frames(match_id: str, frames_per_chunk: int = 600):
    match_path = Path(f"data/processed/match_{match_id}")
    match_path.mkdir(parents=True, exist_ok=True)
    player_indices, frames = get_match_object(match_id=match_id, hz=20)

    for chunk_index, chunk in chunk_frames(frames, frames_per_chunk):
        chunk_path = match_path / f"frames_chunk_{chunk_index:04d}.npy"

        if isinstance(chunk, np.ndarray):
            chunk_array = chunk
        else:
            chunk_array = np.stack(chunk, axis=0)

        np.save(chunk_path, chunk_array)


def load_match_frames(match_id: str):
    pass