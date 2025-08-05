"""
Helper functions and utilities
"""
import re
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
import asyncio
from functools import wraps
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
            attempt=retry_state.attempt_number,
            wait_time=retry_state.next_action.sleep
        )
    )


def log_execution_time(func):
    """Log function execution time"""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = datetime.utcnow()
        try:
            result = await func(*args, **kwargs)
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            logger.debug(
                f"{func.__name__} executed",
                execution_time=execution_time
            )
            return result
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            logger.error(
                f"{func.__name__} failed",
                execution_time=execution_time,
                error=str(e)
            )
            raise

    return wrapper


# ============ URL HELPERS ============

def parse_telegram_url(url: str) -> Optional[Tuple[str, int]]:
    """
    Parse Telegram URL to extract channel and post ID

    Args:
        url: Telegram post URL

    Returns:
        Tuple of (channel_username, post_id) or None
    """
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
    # Remove @ if present
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


def is_within_time_window(
        check_time: datetime,
        start_hour: int = 8,
        end_hour: int = 23
) -> bool:
    """Check if time is within allowed window (in UTC)"""
    hour = check_time.hour
    return start_hour <= hour < end_hour


# ============ QUANTITY HELPERS ============

def calculate_portions(
        total: int,
        portions: int = 5,
        front_loaded: bool = True
) -> List[int]:
    """
    Distribute total quantity across portions

    Args:
        total: Total quantity to distribute
        portions: Number of portions
        front_loaded: If True, larger portions at start

    Returns:
        List of quantities per portion
    """
    if portions <= 0 or total <= 0:
        return []

    if portions == 1:
        return [total]

    if front_loaded:
        # 70% in first 4 portions, 30% in last
        if portions == 5:
            first_four = int(total * 0.7)
            last_one = total - first_four

            # Distribute first four with decreasing amounts
            p1 = int(first_four * 0.35)  # 35% of 70%
            p2 = int(first_four * 0.27)  # 27% of 70%
            p3 = int(first_four * 0.22)  # 22% of 70%
            p4 = first_four - p1 - p2 - p3  # Rest

            return [p1, p2, p3, p4, last_one]

    # Even distribution
    base = total // portions
    remainder = total % portions

    result = [base] * portions
    # Add remainder to first portions
    for i in range(remainder):
        result[i] += 1

    return result


def validate_quantity_limits(
        quantity: int,
        min_limit: int,
        max_limit: int
) -> Tuple[bool, Optional[str]]:
    """
    Validate quantity against service limits

    Returns:
        Tuple of (is_valid, error_message)
    """
    if quantity < min_limit:
        return False, f"Quantity {quantity} is below minimum {min_limit}"

    if quantity > max_limit:
        return False, f"Quantity {quantity} exceeds maximum {max_limit}"

    return True, None


# ============ BATCH PROCESSING ============

async def process_in_batches(
        items: List[Any],
        batch_size: int,
        process_func,
        delay_between_batches: float = 1.0
) -> List[Any]:
    """
    Process items in batches with delay

    Args:
        items: List of items to process
        batch_size: Size of each batch
        process_func: Async function to process each item
        delay_between_batches: Delay in seconds between batches

    Returns:
        List of results
    """
    results = []

    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]

        # Process batch concurrently
        batch_results = await asyncio.gather(
            *[process_func(item) for item in batch],
            return_exceptions=True
        )

        results.extend(batch_results)

        # Delay between batches (except for last batch)
        if i + batch_size < len(items):
            await asyncio.sleep(delay_between_batches)

    return results


# ============ VALIDATION ============

def validate_telegram_username(username: str) -> bool:
    """Validate Telegram username format"""
    # Remove @ if present
    username = username.lstrip('@')

    # Username rules:
    # - 5-32 characters
    # - Can contain a-z, 0-9, underscores
    # - Must start with a letter
    pattern = r'^[a-zA-Z][a-zA-Z0-9_]{4,31}$'
    return bool(re.match(pattern, username))


def validate_telegram_channel_id(channel_id: int) -> bool:
    """Validate Telegram channel ID"""
    # Channel IDs are negative and start with -100
    return channel_id < 0 and str(channel_id).startswith('-100')


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


# ============ ERROR HELPERS ============

def safe_dict_get(
        dictionary: Dict[str, Any],
        path: str,
        default: Any = None
) -> Any:
    """
    Safely get nested dictionary value

    Args:
        dictionary: Dictionary to search
        path: Dot-separated path (e.g., 'user.profile.name')
        default: Default value if not found

    Returns:
        Value at path or default
    """
    keys = path.split('.')
    value = dictionary

    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default

    return value


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


# ============ STATISTICS ============

def calculate_cost(
        quantity: int,
        price_per_1000: float
) -> float:
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