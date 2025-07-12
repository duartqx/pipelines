from datetime import datetime

import json
import logging
import sys
from typing import Literal


class ISOFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None) -> str:
        return datetime.fromtimestamp(record.created).astimezone().isoformat()


logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
formatter = ISOFormatter(
    fmt='{"level": "%(levelname)s", "timestamp": "%(asctime)s", "event": %(message)s }'
)
handler.setFormatter(formatter)
logger.addHandler(handler)


async def slogger(level: Literal["INFO", "ERROR"], **kwargs) -> None:
    getattr(logger, level.lower())(json.dumps(kwargs))
