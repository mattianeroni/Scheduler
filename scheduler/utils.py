import sys
import logging
from pathlib import Path

def setup_logging(output_path: Path):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)-12s | %(levelname)-8s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(output_path / "project_debug.log")
        ]
    )