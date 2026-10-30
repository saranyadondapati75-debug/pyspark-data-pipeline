
import logging
from pathlib import Path
from datetime import datetime, timezone
from uuid import uuid4


def create_run_logger():
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    run_id = str(uuid4())
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"pipeline_{timestamp}_{run_id[:8]}.log"

    logger = logging.getLogger(f"pipeline_{run_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info("Pipeline run started. run_id=%s", run_id)

    return logger, run_id

