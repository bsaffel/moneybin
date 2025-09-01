# MoneyBin Centralized Logging

This package provides unified logging configuration for all MoneyBin components following standard Python logging patterns.

## Quick Start

### Basic Usage

```python
import logging
from src.logging import setup_logging

# Configure logging once at application startup
setup_logging()

# Get logger in each module using standard pattern
logger = logging.getLogger(__name__)
logger.info("This is an info message")
logger.error("This is an error message")
```

### CLI Applications

```python
import logging
from src.logging import setup_logging

# Set up CLI-friendly logging
setup_logging(cli_mode=True, verbose=False)
logger = logging.getLogger(__name__)
```

### Dagster Assets

```python
from dagster import asset, get_dagster_logger

@asset
def my_asset():
    logger = get_dagster_logger()  # Use Dagster's own logger
    logger.info("Processing asset...")
```

## Configuration

### Environment Variables

- `LOG_LEVEL`: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL) - default: INFO
- `LOG_TO_FILE`: Enable file logging (true/false) - default: true
- `LOG_FILE_PATH`: Path to log file - default: logs/moneybin.log
- `LOG_MAX_FILE_SIZE_MB`: Max log file size in MB - default: 50
- `LOG_BACKUP_COUNT`: Number of backup log files - default: 5

### Programmatic Configuration

```python
from src.logging import LoggingConfig, setup_logging

config = LoggingConfig(
    level="DEBUG",
    log_to_file=True,
    log_file_path=Path("custom/path.log")
)

setup_logging(config)
```

## Features

- **Unified Configuration**: Single source of truth for all logging settings
- **Environment-based**: Configure via environment variables
- **File Rotation**: Automatic log file rotation with size limits
- **CLI Mode**: Simplified formatting for command-line tools
- **Dual Output**: Logs to both stdout AND file simultaneously when enabled
- **Standard Python Patterns**: Uses `logging.getLogger(__name__)` idiomatically

## Migration

The following modules have been updated to use centralized logging:

- `src/cli/commands/extract.py`
- `src/cli/commands/credentials.py`
- `src/extractors/plaid_extractor.py`
- `src/utils/secrets_manager.py`
- `pipelines/assets.py`

## Log Output

### Standard Format

```text
2025-09-01 10:07:12,615 - module.name - INFO - Message content
```

### CLI Format (simplified)

```text
Message content
```
