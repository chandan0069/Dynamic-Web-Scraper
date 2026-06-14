from __future__ import annotations

import logging
import sys
from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_database: str = "web_scraper"
    log_level: str = "INFO"
    scrape_output_dir: str = "output"
    default_scrape_interval: int = 300
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    registry_reload_interval: int = 30

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache()
def get_settings() -> Settings:
    return Settings()


class _BoundLogger:

    def __init__(self, logger: logging.Logger, **context):
        self._logger = logger
        self._context = context

    def bind(self, **kwargs) -> "_BoundLogger":
        return _BoundLogger(self._logger, **{**self._context, **kwargs})

    def _format(self, msg: str, **extra) -> str:
        ctx = {**self._context, **extra}
        if not ctx:
            return msg
        parts = " | ".join(f"{k}={v}" for k, v in ctx.items())
        return f"{msg} [{parts}]"

    def debug(self, msg: str, **kwargs) -> None:
        self._logger.debug(self._format(msg, **kwargs))

    def info(self, msg: str, **kwargs) -> None:
        self._logger.info(self._format(msg, **kwargs))

    def warning(self, msg: str, **kwargs) -> None:
        self._logger.warning(self._format(msg, **kwargs))

    def error(self, msg: str, **kwargs) -> None:
        exc_info = kwargs.pop("exc_info", False)
        self._logger.error(self._format(msg, **kwargs), exc_info=exc_info)

    def critical(self, msg: str, **kwargs) -> None:
        exc_info = kwargs.pop("exc_info", False)
        self._logger.critical(self._format(msg, **kwargs), exc_info=exc_info)


def setup_logging() -> None:
    settings = get_settings()
    level = logging.getLevelName(settings.log_level.upper())
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )


def get_logger(name: str = __name__, **context) -> _BoundLogger:
    return _BoundLogger(logging.getLogger(name), **context)
