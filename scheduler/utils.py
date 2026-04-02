import sys
import logging
from pathlib import Path

def setup_logging(output_path: Path):
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s][%(name)s][%(levelname)s] - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(output_path / "project_debug.log")
        ]
    )