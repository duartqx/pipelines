from abc import abstractmethod
from contextlib import asynccontextmanager
from datetime import datetime

import json
import logging
import sys
from typing import (
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Coroutine,
    Literal,
    Protocol,
)

__all__ = ["SLogger", "SLoggable", "Slog"]


class ISOFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None) -> str:
        return datetime.fromtimestamp(record.created).astimezone().isoformat()


def get_logger() -> logging.Logger:

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = ISOFormatter(
        fmt=(
            # message: é um dicionário
            # fmt: off
            '{'
                '"level": "%(levelname)s", '
                '"timestamp": "%(asctime)s", '
                '"event": %(message)s '
            '}'
            # fmt: on
        )
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


logger = get_logger()

type Slog = dict[str, str]


class SLoggable(Protocol):
    @abstractmethod
    def slog(self) -> Slog: ...


class SLogger[T]:
    def __init__(self, skip_enter: bool = False) -> None:
        self.skip_enter = skip_enter

    async def info(self, **kwargs) -> None:
        logger.info(json.dumps(kwargs))

    async def error(self, **kwargs) -> None:
        logger.error(json.dumps(kwargs))

    async def __call__(
        self, slog: Slog
    ) -> AsyncContextManager[Callable[[T], Coroutine[None, None, T]]]:
        if not self.skip_enter:
            await self.info(**slog, status="starting")

        @asynccontextmanager
        async def manager() -> AsyncIterator[Callable[[T], Coroutine[None, None, T]]]:
            async def slogger(result: T) -> T:
                await self.info(**slog, result=result, status="success")

                return result

            try:
                yield slogger
            except Exception as e:
                await self.error(
                    **slog, result=e.__class__.__name__, detail=str(e), status="error"
                )
                raise e

        return manager()


async def slogger(level: Literal["INFO", "ERROR"], **kwargs) -> None:
    getattr(logger, level.lower())(json.dumps(kwargs))
