from datetime import datetime

import json
import logging
import sys
from typing import Any, Literal, Self

__all__ = ["SLogger"]


class ISOFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None) -> str:
        return datetime.fromtimestamp(record.created).astimezone().isoformat()


def get_logger() -> logging.Logger:

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = ISOFormatter(
        fmt='{"level": "%(levelname)s", "timestamp": "%(asctime)s", "event": %(message)s }'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


logger = get_logger()


class SLogger[T]:
    def __init__(self, slog, skip_enter: bool = False) -> None:
        self.slog = slog
        self.skip_enter = skip_enter

    async def info(self, **kwargs) -> None:
        logger.info(json.dumps(kwargs))

    async def error(self, **kwargs) -> None:
        logger.error(json.dumps(kwargs))

    async def __call__(self, result: T) -> T:

        await self.info(**self.slog, result=result, status="success")

        return result

    async def __aenter__(self) -> Self:
        if not self.skip_enter:
            await self.info(**self.slog, status="starting")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> Any:
        if exc is not None:
            return await self.error(
                **self.slog, result=exc_type.__name__, detail=str(exc), status="error"
            )


async def slogger(level: Literal["INFO", "ERROR"], **kwargs) -> None:
    getattr(logger, level.lower())(json.dumps(kwargs))
