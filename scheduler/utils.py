import sys
import logging
from pathlib import Path

def setup_logging(output_path: Path):
    log_file = output_path / "project_debug.log"
    log_file.unlink(missing_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s][%(name)s][%(levelname)s] - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file)
        ]
    )