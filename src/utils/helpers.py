"""
Helper functions and utilities - SIMPLIFIED VERSION
"""
import re
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
import asyncio
from functools import wraps
import hashlib
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

from src.utils.logger import get_logger

logger = get_logger(__name__)


# ============ DECORATORS ============

def async_retry(
    max_attempts: int = 3,
    wait_multiplier: int = 2,
    exceptions: tuple = (Exception,)
):
    """Retry decorator for async functions"""
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=wait_multiplier, min=2, max=30),
        retry=retry_if_exception_type(exceptions),
        before_sleep=lambda retry_state: logger.warning(
            f"Retrying {retry_state.fn.__name__}",
            attempt=retry_state.attempt_number
        )
    )


# ============ URL HELPERS ============

def parse_telegram_url(url: str) -> Optional[Tuple[str, int]]:
    """Parse Telegram URL to extract channel and post ID"""
    patterns = [
        r't\.me/([a-zA-Z0-9_]+)/(\d+)',
        r'telegram\.me/([a-zA-Z0-9_]+)/(\d+)',
        r't\.me/c/(\d+)/(\d+)',  # Private channel
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1), int(match.group(2))

    return None


def build_telegram_url(channel: str, post_id: int) -> str:
    """Build Telegram post URL"""
    channel = channel.lstrip('@')
    return f"https://t.me/{channel}/{post_id}"


# ============ TIME HELPERS ============

def get_schedule_time(delay_minutes: int) -> datetime:
    """Get scheduled time from delay in minutes"""
    return datetime.utcnow() + timedelta(minutes=delay_minutes)


def format_duration(seconds: int) -> str:
    """Format duration in seconds to human readable string"""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}m {secs}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"


# ============ VALIDATION ============

def validate_telegram_username(username: str) -> bool:
    """Validate Telegram username format"""
    username = username.lstrip('@')
    pattern = r'^[a-zA-Z][a-zA-Z0-9_]{4,31}$'
    return bool(re.match(pattern, username))


def validate_telegram_channel_id(channel_id: int) -> bool:
    """Validate Telegram channel ID"""
    return channel_id < 0 and str(channel_id).startswith('-100')


def validate_post_url(url: str) -> bool:
    """Validate Telegram post URL"""
    return bool(parse_telegram_url(url))


def validate_quantity_limits(
    quantity: int,
    min_limit: int,
    max_limit: int
) -> Tuple[bool, Optional[str]]:
    """Validate quantity against service limits"""
    if quantity < min_limit:
        return False, f"Quantity {quantity} is below minimum {min_limit}"

    if quantity > max_limit:
        return False, f"Quantity {quantity} exceeds maximum {max_limit}"

    return True, None


def adjust_quantity_to_limits(
    quantity: int,
    min_limit: int,
    max_limit: int
) -> int:
    """Adjust quantity to fit within limits"""
    if quantity < min_limit:
        return min_limit
    elif quantity > max_limit:
        return max_limit
    return quantity


# ============ FORMATTING ============

def format_number(num: int) -> str:
    """Format number with thousand separators"""
    return f"{num:,}"


def format_price(amount: float, currency: str = "USD") -> str:
    """Format price with currency"""
    if currency == "USD":
        return f"${amount:.2f}"
    return f"{amount:.2f} {currency}"


def truncate_text(text: str, max_length: int = 100) -> str:
    """Truncate text to max length with ellipsis"""
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


def format_percentage(value: float, decimals: int = 1) -> str:
    """Format percentage value"""
    return f"{value:.{decimals}f}%"


# ============ COST CALCULATIONS ============

def calculate_cost(quantity: int, price_per_1000: float) -> float:
    """Calculate cost for given quantity"""
    return (quantity / 1000) * price_per_1000


def calculate_total_cost(
    views: int,
    reactions: int,
    reposts: int,
    prices: Dict[str, float]
) -> Dict[str, float]:
    """Calculate total costs breakdown"""
    return {
        'views': calculate_cost(views, prices.get('views', 0)),
        'reactions': calculate_cost(reactions, prices.get('reactions', 0)),
        'reposts': calculate_cost(reposts, prices.get('reposts', 0)),
        'total': (
            calculate_cost(views, prices.get('views', 0)) +
            calculate_cost(reactions, prices.get('reactions', 0)) +
            calculate_cost(reposts, prices.get('reposts', 0))
        )
    }


# ============ ERROR HELPERS ============

def extract_error_message(error: Exception) -> str:
    """Extract clean error message from exception"""
    error_str = str(error)

    # Remove common prefixes
    prefixes = ['Error: ', 'Exception: ', 'Failed: ']
    for prefix in prefixes:
        if error_str.startswith(prefix):
            error_str = error_str[len(prefix):]

    return error_str.strip()


class ErrorRecovery:
    """Helper for error recovery strategies"""

    @staticmethod
    def should_retry_error(error: Exception) -> bool:
        """Determine if error should be retried"""
        error_str = str(error).lower()

        retry_errors = [
            'timeout',
            'connection',
            'rate limit',
            'temporary',
            'try again',
            '429',
            '503',
            '504',
        ]

        return any(err in error_str for err in retry_errors)

    @staticmethod
    def categorize_error(error: Exception) -> str:
        """Categorize error for handling"""
        error_str = str(error).lower()

        if 'insufficient funds' in error_str or 'balance' in error_str:
            return 'insufficient_funds'
        elif 'invalid' in error_str and 'key' in error_str:
            return 'invalid_api_key'
        elif 'quantity' in error_str:
            return 'invalid_quantity'
        elif 'service' in error_str and 'not found' in error_str:
            return 'service_not_found'
        elif 'rate limit' in error_str or '429' in error_str:
            return 'rate_limit'
        elif 'timeout' in error_str:
            return 'timeout'
        elif 'connection' in error_str:
            return 'connection_error'
        else:
            return 'unknown'


# ============ RATE LIMITING ============

class RateLimiter:
    """Simple rate limiter for API calls"""

    def __init__(self, calls_per_minute: int = 60):
        self.calls_per_minute = calls_per_minute
        self.calls = []
        self.window = 60  # seconds

    async def acquire(self):
        """Wait if necessary to respect rate limit"""
        now = datetime.utcnow()

        # Remove old calls outside window
        cutoff = now - timedelta(seconds=self.window)
        self.calls = [call_time for call_time in self.calls if call_time > cutoff]

        # Check if we're at limit
        if len(self.calls) >= self.calls_per_minute:
            # Calculate wait time
            oldest_call = min(self.calls)
            wait_until = oldest_call + timedelta(seconds=self.window)
            wait_seconds = (wait_until - now).total_seconds()

            if wait_seconds > 0:
                logger.warning(f"Rate limit reached, waiting {wait_seconds:.1f}s")
                await asyncio.sleep(wait_seconds)

        # Record this call
        self.calls.append(now)

    def reset(self):
        """Reset rate limiter"""
        self.calls.clear()


# ============ SERVICE HELPERS ============

def get_reaction_emoji_from_service_name(service_name: str) -> str:
    """Extract emoji from service name"""
    import re

    # Common patterns
    emoji_pattern = r'[^\w\s,.\-()]{1,2}'
    emojis = re.findall(emoji_pattern, service_name)

    if emojis:
        return emojis[0]

    # Check for text descriptions
    if 'сердце' in service_name.lower() or 'heart' in service_name.lower():
        return '❤️'
    elif 'огонь' in service_name.lower() or 'fire' in service_name.lower():
        return '🔥'
    elif 'лайк' in service_name.lower() or 'like' in service_name.lower():
        return '👍'

    return 'mix'


# ============ PORTION HELPERS ============

def validate_portion_distribution(portions: List[Dict[str, Any]], total_target: int) -> bool:
    """Validate that portions sum up correctly"""
    total_calculated = sum(
        p.get('quantity_per_run', 0) * p.get('runs', 1)
        for p in portions
    )

    # Allow 5% deviation
    deviation = abs(total_calculated - total_target) / total_target if total_target > 0 else 0
    return deviation <= 0.05


# ============ DATA HELPERS ============

def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split list into chunks"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def clean_dict(d: Dict[str, Any], remove_none: bool = True) -> Dict[str, Any]:
    """Clean dictionary by removing None values"""
    cleaned = {}

    for key, value in d.items():
        if remove_none and value is None:
            continue
        cleaned[key] = value

    return cleaned


# ============ ASYNC HELPERS ============

async def run_with_timeout(coro, timeout: int, default: Any = None):
    """Run coroutine with timeout"""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Coroutine timed out after {timeout}s")
        return default


# ============ PERFORMANCE MONITOR (SIMPLIFIED) ============

class PerformanceMonitor:
    """Simple performance monitor"""

    def __init__(self):
        self.metrics = {}

    async def measure(self, name: str, func, *args, **kwargs):
        """Measure function execution time"""
        start = datetime.utcnow()
        try:
            result = await func(*args, **kwargs)
            duration = (datetime.utcnow() - start).total_seconds()

            if name not in self.metrics:
                self.metrics[name] = []

            self.metrics[name].append({
                'duration': duration,
                'success': True,
                'timestamp': datetime.utcnow()
            })

            return result

        except Exception as e:
            duration = (datetime.utcnow() - start).total_seconds()

            if name not in self.metrics:
                self.metrics[name] = []

            self.metrics[name].append({
                'duration': duration,
                'success': False,
                'timestamp': datetime.utcnow()
            })

            raise

    def get_stats(self, name: str) -> Dict[str, Any]:
        """Get statistics for metric"""
        if name not in self.metrics:
            return {}

        data = self.metrics[name]
        durations = [m['duration'] for m in data]
        successes = [m for m in data if m['success']]

        return {
            'count': len(data),
            'success_count': len(successes),
            'success_rate': len(successes) / len(data) if data else 0,
            'avg_duration': sum(durations) / len(durations) if durations else 0
        }

    def clear_metrics(self):
        """Clear metrics"""
        self.metrics.clear()