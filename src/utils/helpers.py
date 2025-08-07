"""
Helper utilities for the bot
"""
import re
import hashlib
import random
import string
import asyncio
import functools
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
from urllib.parse import urlparse


def extract_channel_id(text: str) -> Optional[int]:
    """
    Extract channel ID from various formats
    Examples:
    - -1001234567890
    - https://t.me/c/1234567890/123
    - @channelname (returns None - needs API lookup)
    """
    # Direct channel ID
    if text.startswith("-100"):
        try:
            return int(text)
        except ValueError:
            pass

    # From t.me link
    if "t.me/c/" in text:
        match = re.search(r't\.me/c/(\d+)', text)
        if match:
            # Private channel links don't have -100 prefix
            channel_id = match.group(1)
            return int(f"-100{channel_id}")

    # Username format
    if text.startswith("@"):
        return None  # Needs API lookup

    try:
        return int(text)
    except ValueError:
        return None


def format_number(num: Union[int, float]) -> str:
    """
    Format number with K/M suffix
    1000 -> 1K
    1500000 -> 1.5M
    """
    if num < 1000:
        return str(num)
    elif num < 1000000:
        return f"{num/1000:.1f}K"
    else:
        return f"{num/1000000:.1f}M"


def calculate_percentage(part: int, total: int) -> float:
    """Calculate percentage safely"""
    if total == 0:
        return 0.0
    return (part / total) * 100


def generate_random_delay(min_seconds: int = 1, max_seconds: int = 5) -> float:
    """Generate random delay in seconds"""
    return random.uniform(min_seconds, max_seconds)


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split list into chunks"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def clean_html(text: str) -> str:
    """Remove HTML tags from text"""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)


def truncate_text(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """Truncate text to maximum length"""
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def is_valid_telegram_link(url: str) -> bool:
    """Check if URL is valid Telegram link"""
    try:
        parsed = urlparse(url)
        return parsed.netloc in ["t.me", "telegram.me", "telegram.dog"]
    except:
        return False


def extract_post_id_from_link(link: str) -> Optional[int]:
    """
    Extract post ID from Telegram link
    https://t.me/c/1234567890/456 -> 456
    """
    match = re.search(r't\.me/c/\d+/(\d+)', link)
    if match:
        return int(match.group(1))
    return None


def generate_unique_id(prefix: str = "") -> str:
    """Generate unique identifier"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"{prefix}{timestamp}_{random_str}"


def calculate_eta(
    current: int,
    total: int,
    elapsed_seconds: float
) -> Optional[timedelta]:
    """Calculate estimated time of arrival"""
    if current == 0 or elapsed_seconds == 0:
        return None

    rate = current / elapsed_seconds
    remaining = total - current

    if rate == 0:
        return None

    eta_seconds = remaining / rate
    return timedelta(seconds=int(eta_seconds))


def format_duration(seconds: float) -> str:
    """
    Format duration in human readable format
    3665 -> "1h 1m 5s"
    """
    if seconds < 60:
        return f"{seconds:.1f}s"

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")

    return " ".join(parts)


def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe storage"""
    # Remove invalid characters
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    # Replace spaces with underscores
    filename = filename.replace(' ', '_')
    # Limit length
    if len(filename) > 255:
        name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
        filename = name[:250] + ('.' + ext if ext else '')
    return filename


def parse_emoji_distribution(text: str) -> Dict[str, int]:
    """
    Parse emoji distribution from text
    "👍:45,❤️:30,🔥:25" -> {"👍": 45, "❤️": 30, "🔥": 25}
    """
    distribution = {}

    if not text:
        return distribution

    parts = text.split(',')
    for part in parts:
        if ':' in part:
            emoji, percentage = part.split(':', 1)
            try:
                distribution[emoji.strip()] = int(percentage.strip())
            except ValueError:
                continue

    return distribution


def validate_distribution(distribution: Dict[str, int]) -> bool:
    """Validate that distribution sums to 100%"""
    total = sum(distribution.values())
    return 95 <= total <= 105  # Allow small rounding errors


def retry_with_backoff(
    func,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0
):
    """
    Decorator for retry with exponential backoff
    """
    import functools
    import asyncio

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        delay = initial_delay
        last_exception = None

        for attempt in range(max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)
                    delay *= backoff_factor

        raise last_exception

    return wrapper


class RateLimiter:
    """Simple rate limiter"""

    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.calls = []

    async def acquire(self):
        """Wait if rate limit exceeded"""
        now = datetime.now()

        # Remove old calls
        self.calls = [
            call_time for call_time in self.calls
            if (now - call_time).total_seconds() < self.period
        ]

        # Check rate limit
        if len(self.calls) >= self.max_calls:
            # Calculate wait time
            oldest_call = min(self.calls)
            wait_time = self.period - (now - oldest_call).total_seconds()

            if wait_time > 0:
                await asyncio.sleep(wait_time)

        # Record this call
        self.calls.append(now)


def mask_sensitive_data(text: str) -> str:
    """Mask sensitive data in logs"""
    # Mask API keys
    text = re.sub(r'([A-Za-z0-9]{20,})', lambda m: m.group()[:6] + '...' + m.group()[-4:], text)
    # Mask tokens
    text = re.sub(r'token["\']?\s*[:=]\s*["\']?([^"\'\s]+)', 'token=***', text, flags=re.IGNORECASE)
    # Mask passwords
    text = re.sub(r'password["\']?\s*[:=]\s*["\']?([^"\'\s]+)', 'password=***', text, flags=re.IGNORECASE)
    return text