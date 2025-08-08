"""
Helper functions and utilities - Complete implementation
"""
import re
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
import asyncio
from functools import wraps
from decimal import Decimal
import hashlib
import json
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


def cache_result(ttl_seconds: int = 300):
    """Cache async function results"""
    def decorator(func):
        cache = {}

        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Create cache key
            key = hashlib.md5(
                f"{args}{kwargs}".encode()
            ).hexdigest()

            # Check cache
            if key in cache:
                cached_time, cached_result = cache[key]
                if (datetime.utcnow() - cached_time).total_seconds() < ttl_seconds:
                    return cached_result

            # Execute function
            result = await func(*args, **kwargs)

            # Store in cache
            cache[key] = (datetime.utcnow(), result)

            return result

        wrapper.clear_cache = lambda: cache.clear()
        return wrapper

    return decorator


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


def extract_channel_id_from_url(url: str) -> Optional[int]:
    """Extract channel ID from various Telegram URL formats"""
    patterns = [
        r't\.me/c/(\d+)/\d+',  # Private channel format
        r'telegram\.me/c/(\d+)/\d+',
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            # Private channels have format -100XXXXXXXXXX
            channel_id = f"-100{match.group(1)}"
            return int(channel_id)

    return None


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


def get_time_until_next_window(
    current_time: datetime,
    start_hour: int = 8
) -> timedelta:
    """Get time until next allowed window"""
    if is_within_time_window(current_time, start_hour):
        return timedelta(0)

    # Calculate next window start
    next_start = current_time.replace(
        hour=start_hour,
        minute=0,
        second=0,
        microsecond=0
    )

    # If start time has passed today, move to tomorrow
    if next_start <= current_time:
        next_start += timedelta(days=1)

    return next_start - current_time


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


# ============ TELEGRAM SPECIFIC ============

def is_channel_public(channel_id: int) -> bool:
    """Check if channel is public based on ID"""
    # Public channels don't have -100 prefix in their actual ID
    # But we store all channels with -100 prefix for consistency
    return True  # This is simplified - in reality would need API check


def normalize_channel_username(username: str) -> str:
    """Normalize channel username"""
    # Remove @ symbol
    username = username.lstrip('@')

    # Convert to lowercase
    username = username.lower()

    # Remove any trailing slashes or spaces
    username = username.strip('/ ')

    return username


def get_channel_type(channel_id: int) -> str:
    """Determine channel type from ID"""
    channel_id_str = str(channel_id)

    if channel_id_str.startswith('-100'):
        return 'supergroup'
    elif channel_id_str.startswith('-'):
        return 'group'
    else:
        return 'user'


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


def validate_post_url(url: str) -> bool:
    """Validate Telegram post URL"""
    return bool(parse_telegram_url(url))


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


def extract_error_message(error: Exception) -> str:
    """Extract clean error message from exception"""
    error_str = str(error)

    # Remove common prefixes
    prefixes = [
        'Error: ',
        'Exception: ',
        'Failed: ',
    ]

    for prefix in prefixes:
        if error_str.startswith(prefix):
            error_str = error_str[len(prefix):]

    return error_str.strip()


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


def calculate_success_rate(
    completed: int,
    total: int
) -> float:
    """Calculate success rate percentage"""
    if total == 0:
        return 0.0
    return (completed / total) * 100


# ============ DRIP FEED CALCULATIONS ============

def calculate_drip_feed_timing(
    total_quantity: int,
    portions: int = 5,
    total_hours: int = 24
) -> List[Dict[str, int]]:
    """
    Calculate optimal drip-feed timing

    Returns list of portions with timing:
    - 70% in first 3-5 hours
    - 30% spread over remaining time
    """
    if portions < 2:
        return [{
            'quantity': total_quantity,
            'delay_minutes': 0,
            'interval_minutes': 0,
            'runs': 1
        }]

    # Calculate quantities
    fast_portion = int(total_quantity * 0.7)  # 70%
    slow_portion = total_quantity - fast_portion  # 30%

    # Fast portions (first 4)
    fast_quantities = calculate_portions(fast_portion, portions - 1, front_loaded=True)

    # Intervals for fast portions (increasing)
    fast_intervals = [15, 18, 22, 26][:portions - 1]

    result = []

    # Add fast portions
    for i, qty in enumerate(fast_quantities):
        # Calculate runs to stay within quantity limits
        if qty > 150:  # If portion is large
            runs = qty // 100  # 100 per run
            quantity_per_run = qty // runs
            if qty % runs > 0:
                runs += 1
        else:
            runs = 1
            quantity_per_run = qty

        result.append({
            'quantity': quantity_per_run,
            'delay_minutes': 0,  # All start immediately
            'interval_minutes': fast_intervals[i] if i < len(fast_intervals) else 20,
            'runs': runs
        })

    # Add slow portion (last one)
    if slow_portion > 0:
        # Calculate delay for slow portion (after fast portions complete)
        fast_duration = sum(
            p['interval_minutes'] * (p['runs'] - 1)
            for p in result
        ) / 60  # Convert to hours

        delay_hours = max(3, fast_duration)  # At least 3 hours delay

        # Slow portion spread over remaining time
        remaining_hours = total_hours - delay_hours
        slow_runs = max(1, int(remaining_hours * 2))  # 2 runs per hour

        result.append({
            'quantity': max(1, slow_portion // slow_runs),
            'delay_minutes': int(delay_hours * 60),
            'interval_minutes': max(15, int(remaining_hours * 60 // slow_runs)),
            'runs': slow_runs
        })

    return result


def validate_portion_distribution(portions: List[Dict[str, Any]], total_target: int) -> bool:
    """Validate that portions sum up correctly"""
    total_calculated = sum(
        p.get('quantity_per_run', 0) * p.get('runs', 1)
        for p in portions
    )

    # Allow 5% deviation
    deviation = abs(total_calculated - total_target) / total_target
    return deviation <= 0.05


# ============ SERVICE HELPERS ============

def get_reaction_emoji_from_service_name(service_name: str) -> str:
    """Extract emoji from service name"""
    import re

    # Common patterns
    emoji_pattern = r'[^\w\s,.\-()]{1,2}'
    emojis = re.findall(emoji_pattern, service_name)

    if emojis:
        # Return first emoji found
        return emojis[0]

    # Check for text descriptions
    if 'сердце' in service_name.lower() or 'heart' in service_name.lower():
        return '❤️'
    elif 'огонь' in service_name.lower() or 'fire' in service_name.lower():
        return '🔥'
    elif 'лайк' in service_name.lower() or 'like' in service_name.lower():
        return '👍'

    return 'mix'


def parse_service_limits(service_name: str) -> Dict[str, Any]:
    """Parse service limits from name"""
    result = {
        'has_refill': 'R' in service_name,
        'refill_days': None,
        'is_premium': 'premium' in service_name.lower(),
        'is_instant': 'моментальн' in service_name.lower() or 'instant' in service_name.lower(),
        'country': None
    }

    # Extract refill days
    import re
    refill_match = re.search(r'R(\d+)', service_name)
    if refill_match:
        result['refill_days'] = int(refill_match.group(1))

    # Extract country
    countries = {
        'UA': 'Ukraine',
        'США': 'USA',
        'СНГ': 'CIS',
        'Казахстан': 'Kazakhstan',
        'EN': 'English'
    }

    for code, name in countries.items():
        if code in service_name:
            result['country'] = name
            break

    return result


# ============ MONITORING HELPERS ============

async def should_process_post(
    post_time: datetime,
    current_time: Optional[datetime] = None
) -> bool:
    """Check if post should be processed based on time"""
    if not current_time:
        current_time = datetime.utcnow()

    post_age = (current_time - post_time).total_seconds() / 3600  # hours

    # Don't process posts older than 48 hours
    if post_age > 48:
        return False

    # Don't process posts from future (clock issues)
    if post_age < 0:
        return False

    return True


def estimate_completion_time(
    portions: List[Dict[str, Any]]
) -> datetime:
    """Estimate when all portions will complete"""
    max_time = datetime.utcnow()

    for portion in portions:
        start_delay = portion.get('start_delay_minutes', 0)
        interval = portion.get('interval_minutes', 0)
        runs = portion.get('runs', 1)

        # Calculate total time for this portion
        portion_duration = start_delay + (interval * (runs - 1))
        portion_end = datetime.utcnow() + timedelta(minutes=portion_duration)

        if portion_end > max_time:
            max_time = portion_end

    return max_time


# ============ ERROR RECOVERY ============

class ErrorRecovery:
    """Helper for error recovery strategies"""

    @staticmethod
    def should_retry_error(error: Exception) -> bool:
        """Determine if error should be retried"""
        error_str = str(error).lower()

        # Temporary errors that should be retried
        retry_errors = [
            'timeout',
            'connection',
            'rate limit',
            'temporary',
            'try again',
            '429',  # Too many requests
            '503',  # Service unavailable
            '504',  # Gateway timeout
        ]

        return any(err in error_str for err in retry_errors)

    @staticmethod
    def get_retry_delay(attempt: int, base_delay: int = 5) -> int:
        """Calculate exponential backoff delay"""
        return min(base_delay * (2 ** attempt), 300)  # Max 5 minutes

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


# ============ PERFORMANCE MONITORING ============

class PerformanceMonitor:
    """Monitor performance metrics"""

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
                'error': str(e),
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
            'avg_duration': sum(durations) / len(durations) if durations else 0,
            'min_duration': min(durations) if durations else 0,
            'max_duration': max(durations) if durations else 0
        }

    def clear_metrics(self, name: Optional[str] = None):
        """Clear metrics"""
        if name:
            self.metrics.pop(name, None)
        else:
            self.metrics.clear()


# ============ DATA HELPERS ============

def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split list into chunks"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def merge_dicts(dict1: Dict, dict2: Dict, deep: bool = True) -> Dict:
    """Merge two dictionaries"""
    result = dict1.copy()

    if not deep:
        result.update(dict2)
        return result

    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dicts(result[key], value, deep=True)
        else:
            result[key] = value

    return result


def clean_dict(d: Dict[str, Any], remove_none: bool = True, remove_empty: bool = False) -> Dict[str, Any]:
    """Clean dictionary by removing None or empty values"""
    cleaned = {}

    for key, value in d.items():
        if remove_none and value is None:
            continue
        if remove_empty and not value and value != 0:
            continue
        cleaned[key] = value

    return cleaned


# ============ JSON HELPERS ============

def safe_json_dumps(obj: Any, default=str) -> str:
    """Safely convert object to JSON string"""
    try:
        return json.dumps(obj, default=default, ensure_ascii=False)
    except Exception as e:
        logger.error(f"JSON serialization failed: {e}")
        return "{}"


def safe_json_loads(json_str: str, default: Any = None) -> Any:
    """Safely parse JSON string"""
    try:
        return json.loads(json_str)
    except Exception as e:
        logger.error(f"JSON parsing failed: {e}")
        return default if default is not None else {}


# ============ ASYNC HELPERS ============

async def run_with_timeout(coro, timeout: int, default: Any = None):
    """Run coroutine with timeout"""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Coroutine timed out after {timeout}s")
        return default


async def gather_with_concurrency(
    tasks: List,
    max_concurrent: int = 10
) -> List[Any]:
    """Run tasks with limited concurrency"""
    semaphore = asyncio.Semaphore(max_concurrent)

    async def run_with_semaphore(task):
        async with semaphore:
            return await task

    return await asyncio.gather(
        *[run_with_semaphore(task) for task in tasks],
        return_exceptions=True
    )