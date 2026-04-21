"""Log formatters for MoneyBin.

HumanFormatter provides human-readable output in two variants:
- "cli": message-only for CLI stderr (no timestamp clutter)
- "full": timestamp + logger + level + message for files and MCP stderr

JSONFormatter provides one JSON object per line for structured log analysis.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Literal


class HumanFormatter(logging.Formatter):
    """Human-readable log formatter with CLI and full variants.

    Args:
        variant: "cli" for message-only, "full" for timestamped output.
    """

    _CLI_FMT = "%(message)s"
    _FULL_FMT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    def __init__(self, variant: Literal["cli", "full"] = "full") -> None:
        """Initialize the formatter with the given variant.

        Args:
            variant: "cli" for message-only, "full" for timestamped output.
        """
        fmt = self._CLI_FMT if variant == "cli" else self._FULL_FMT
        super().__init__(fmt)


# Standard fields that should NOT be copied into the JSON "extra" bucket.
_RESERVED_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)


class JSONFormatter(logging.Formatter):
    """One JSON object per line.

    Output includes ``timestamp``, ``logger``, ``level``, ``message``,
    plus any extra attributes set on the LogRecord.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a single-line JSON object.

        Args:
            record: The log record to format.

        Returns:
            JSON string with log data.
        """
        record.message = record.getMessage()

        obj: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "logger": record.name,
            "level": record.levelname,
            "message": record.message,
        }

        # Copy non-standard attributes as extra fields
        for key, value in record.__dict__.items():
            if key not in _RESERVED_ATTRS and key != "message":
                try:
                    json.dumps(value)  # Only include JSON-serializable values
                    obj[key] = value
                except (TypeError, ValueError):
                    pass

        if record.exc_info and record.exc_info[1] is not None:
            obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(obj, default=str)
