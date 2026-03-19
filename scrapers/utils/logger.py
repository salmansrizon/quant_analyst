import logging
import os
from pathlib import Path


class Log:
    """Simple logger that writes to both console and a rotating file."""

    def __init__(self, name: str, filename: str = None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        if self.logger.handlers:
            return  # already configured (e.g. in Dagster context)

        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)-8s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        # File handler (optional)
        if filename:
            Path(filename).parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(filename, encoding="utf-8")
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

    # Convenience proxies
    def debug(self, msg):   self.logger.debug(msg)
    def info(self, msg):    self.logger.info(msg)
    def warning(self, msg): self.logger.warning(msg)
    def error(self, msg):   self.logger.error(msg)
    def critical(self, msg):self.logger.critical(msg)
