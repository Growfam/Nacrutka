"""
Structured logging configuration
"""
import sys
import structlog
from structlog import get_logger as structlog_get_logger
from datetime import datetime
import logging
from typing import Any, Dict

from src.config import settings


def setup_logging():
    """Configure structured logging"""

    # Set log level
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    # Configure processors
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        add_custom_context,
    ]

    # Add color in development
    if settings.environment == "development":
        processors.append(
            structlog.dev.ConsoleRenderer(colors=True)
        )
    else:
        processors.append(
            structlog.processors.JSONRenderer()
        )

    # Configure structlog
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    # Suppress noisy loggers
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def add_custom_context(logger, log_method, event_dict):
    """Add custom context to all log messages"""
    event_dict["environment"] = settings.environment
    event_dict["timestamp"] = datetime.utcnow().isoformat()
    return event_dict


def get_logger(name: str = None):
    """Get configured logger instance"""
    return structlog_get_logger(name)


class LoggerMixin:
    """Mixin class to add logging to any class"""

    @property
    def logger(self):
        if not hasattr(self, "_logger"):
            self._logger = get_logger(self.__class__.__name__)
        return self._logger

    def log_info(self, message: str, **kwargs):
        """Log info message with context"""
        self.logger.info(message, **kwargs)

    def log_error(self, message: str, error: Exception = None, **kwargs):
        """Log error message with exception details"""
        if error:
            kwargs["error_type"] = type(error).__name__
            kwargs["error_message"] = str(error)
        self.logger.error(message, **kwargs)

    def log_warning(self, message: str, **kwargs):
        """Log warning message"""
        self.logger.warning(message, **kwargs)

    def log_debug(self, message: str, **kwargs):
        """Log debug message"""
        self.logger.debug(message, **kwargs)


# Monitoring metrics logger
class MetricsLogger:
    """Log metrics for monitoring"""

    def __init__(self):
        self.logger = get_logger("metrics")

    def log_order_created(self, order_data: Dict[str, Any]):
        """Log order creation"""
        self.logger.info(
            "order_created",
            order_id=order_data.get("id"),
            service_type=order_data.get("service_type"),
            quantity=order_data.get("quantity"),
            channel_id=order_data.get("channel_id")
        )

    def log_api_call(self, api_name: str, method: str, duration: float, success: bool):
        """Log API call metrics"""
        self.logger.info(
            "api_call",
            api=api_name,
            method=method,
            duration_ms=duration * 1000,
            success=success
        )

    def log_post_detected(self, channel_id: int, post_id: int):
        """Log new post detection"""
        self.logger.info(
            "post_detected",
            channel_id=channel_id,
            post_id=post_id
        )

    def log_error_rate(self, error_type: str, count: int):
        """Log error rates"""
        self.logger.warning(
            "error_rate",
            error_type=error_type,
            count=count
        )


# Global metrics logger instance
metrics = MetricsLogger()