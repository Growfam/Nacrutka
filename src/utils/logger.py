"""
Structured logging configuration
"""
import sys
import structlog
from typing import Any, Dict
from datetime import datetime
import json

from src.config import settings


def add_timestamp(_, __, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Add timestamp to log entry"""
    event_dict["timestamp"] = datetime.utcnow().isoformat()
    return event_dict


def setup_logging():
    """Configure structured logging"""

    # Choose renderer based on environment
    if settings.environment == "production":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            add_timestamp,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            renderer,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Get configured logger instance"""
    return structlog.get_logger(name)


# Database logger that saves to logs table
class DatabaseLogger:
    """Logger that saves to database"""

    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = get_logger(__name__)

    async def log(
            self,
            level: str,
            message: str,
            context: Dict[str, Any] = None
    ):
        """Save log entry to database"""
        try:
            query = """
                    INSERT INTO logs (level, message, context)
                    VALUES ($1, $2, $3) \
                    """
            await self.db.execute(
                query,
                level,
                message,
                json.dumps(context or {})
            )
        except Exception as e:
            self.logger.error(
                "Failed to save log to database",
                error=str(e),
                original_message=message
            )

    async def info(self, message: str, **kwargs):
        """Log info message"""
        await self.log("info", message, kwargs)

    async def warning(self, message: str, **kwargs):
        """Log warning message"""
        await self.log("warning", message, kwargs)

    async def error(self, message: str, **kwargs):
        """Log error message"""
        await self.log("error", message, kwargs)


# Setup logging on module import
setup_logging()