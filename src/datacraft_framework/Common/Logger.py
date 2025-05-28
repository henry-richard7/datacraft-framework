# logger_config.py
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from os import getenv
from datetime import datetime

load_dotenv()


class LoggerManager:
    def __init__(
        self,
        process_id: int = 0,
        max_bytes: int = 1_000_000,
        backup_count: int = 5,
        log_level: int = logging.DEBUG,
    ):
        log_path = getenv("lakehouse_framework_home")
        self.log_file = Path(log_path) / "logs" / f"process_id {process_id}.log"
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.log_level = log_level

        self._configure_logging()

    def _configure_logging(self):
        Path(self.log_file).mkdir(parents=True, exist_ok=True)

        logger = logging.getLogger()
        logger.setLevel(self.log_level)

        # Prevent duplicate handlers
        if logger.hasHandlers():
            logger.handlers.clear()

        # Console Handler (INFO and above)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.addFilter(self.ConsoleFilter())
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )

        # Rotating File Handler (DEBUG and above)
        file_handler = RotatingFileHandler(
            self.log_file,
            maxBytes=self.max_bytes,
            backupCount=self.backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        )

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        # 🔽 Add this to mark a new run in logs
        logging.getLogger().info("\n" + "=" * 80)
        logging.getLogger().info(
            "🟢 New run started at: %s\n", datetime.now().isoformat()
        )

    class ConsoleFilter(logging.Filter):
        def filter(self, record):
            # Allow only INFO, WARNING, ERROR, CRITICAL to console
            return record.levelno >= logging.INFO
