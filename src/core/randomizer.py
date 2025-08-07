"""
Randomizer for natural-looking promotion
"""
import random
import hashlib
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

from src.database.models import ServiceType, ChannelSettings
from src.utils.logger import get_logger, LoggerMixin
from src.config import DEFAULT_REACTIONS

logger = get_logger(__name__)


@dataclass
class RandomizedValue:
    """Container for randomized value with metadata"""
    original: int
    randomized: int
    percentage_change: float
    seed: str


class Randomizer(LoggerMixin):
    """Handle randomization for SMM services"""

    def __init__(self):
        self.last_values_cache = {}  # Cache to prevent identical consecutive values
        self.cache_size = 10  # Remember last N values per channel/service

    def randomize_quantity(
            self,
            base_quantity: int,
            randomization_percent: int,
            service_type: ServiceType,
            channel_id: int,
            post_id: int
    ) -> RandomizedValue:
        """
        Randomize quantity with smart distribution

        Rules:
        - Views: NO randomization (always exact)
        - Reactions/Reposts: ±randomization_percent
        - Prevent identical values for last 5 posts
        - Use deterministic seed for debugging
        """

        # Views don't get randomized
        if service_type == ServiceType.VIEWS or randomization_percent == 0:
            return RandomizedValue(
                original=base_quantity,
                randomized=base_quantity,
                percentage_change=0.0,
                seed="no_randomization"
            )

        # Generate seed for reproducibility
        seed = self._generate_seed(channel_id, post_id, service_type)

        # Get cache key
        cache_key = f"{channel_id}_{service_type}"

        # Calculate range
        min_value = int(base_quantity * (1 - randomization_percent / 100))
        max_value = int(base_quantity * (1 + randomization_percent / 100))

        # Generate unique value
        randomized = self._generate_unique_value(
            min_value,
            max_value,
            cache_key,
            seed
        )

        # Calculate actual percentage change
        percentage_change = ((randomized - base_quantity) / base_quantity) * 100

        self.log_debug(
            "Quantity randomized",
            service_type=service_type,
            original=base_quantity,
            randomized=randomized,
            change_percent=f"{percentage_change:+.1f}%",
            seed=seed[:8]
        )

        return RandomizedValue(
            original=base_quantity,
            randomized=randomized,
            percentage_change=percentage_change,
            seed=seed
        )

    def randomize_reaction_distribution(
            self,
            base_distribution: Dict[str, int],
            total_quantity: int,
            channel_id: int,
            post_id: int
    ) -> Dict[str, int]:
        """
        Randomize reaction emoji distribution

        Each emoji percentage gets ±5-15% variation
        But total must equal total_quantity
        """

        if not base_distribution:
            # Use default if not configured
            base_distribution = DEFAULT_REACTIONS

        # Generate seed
        seed = self._generate_seed(channel_id, post_id, "reactions_dist")
        random.seed(seed)

        # Randomize each percentage
        randomized_percentages = {}
        total_percent = 0

        for emoji, base_percent in base_distribution.items():
            # Random variation between -15% to +15% of the base percentage
            # But ensure minimum 5% and maximum 60% for any emoji
            variation = random.uniform(-15, 15)
            new_percent = base_percent + variation
            new_percent = max(5, min(60, new_percent))

            randomized_percentages[emoji] = new_percent
            total_percent += new_percent

        # Normalize to 100%
        if total_percent != 100:
            factor = 100 / total_percent
            for emoji in randomized_percentages:
                randomized_percentages[emoji] = int(randomized_percentages[emoji] * factor)

        # Convert percentages to actual quantities
        result = {}
        remaining = total_quantity

        sorted_emojis = sorted(
            randomized_percentages.items(),
            key=lambda x: x[1],
            reverse=True
        )

        for i, (emoji, percent) in enumerate(sorted_emojis):
            if i == len(sorted_emojis) - 1:
                # Last emoji gets remainder
                quantity = remaining
            else:
                quantity = int(total_quantity * percent / 100)
                remaining -= quantity

            result[emoji] = quantity

        self.log_info(
            "Reaction distribution randomized",
            channel_id=channel_id,
            post_id=post_id,
            total=total_quantity,
            distribution=result
        )

        # Reset random seed
        random.seed()

        return result

    def randomize_intervals(
            self,
            base_interval: int,
            variation_percent: int = 20
    ) -> int:
        """
        Randomize time intervals

        Used for drip-feed intervals
        """
        min_interval = int(base_interval * (1 - variation_percent / 100))
        max_interval = int(base_interval * (1 + variation_percent / 100))

        # Ensure minimum 1 minute
        min_interval = max(1, min_interval)

        return random.randint(min_interval, max_interval)

    def randomize_delay(
            self,
            base_delay_minutes: int,
            max_additional_minutes: int = 3
    ) -> timedelta:
        """
        Randomize start delay

        Used for repost delays
        """
        additional = random.randint(0, max_additional_minutes)
        total_minutes = base_delay_minutes + additional

        # Add some seconds for more natural look
        additional_seconds = random.randint(0, 59)

        return timedelta(minutes=total_minutes, seconds=additional_seconds)

    def generate_portion_sizes(
            self,
            total: int,
            portions_count: int,
            fast_percent: int = 70
    ) -> List[int]:
        """
        Generate randomized portion sizes

        Maintains 70/30 or configured ratio but with variation
        """
        if portions_count == 1:
            return [total]

        # Calculate fast and slow portions
        fast_count = max(1, int(portions_count * 0.7))
        slow_count = portions_count - fast_count

        fast_total = int(total * fast_percent / 100)
        slow_total = total - fast_total

        portions = []

        # Generate fast portions with decreasing sizes
        if fast_count > 0:
            # Base percentages for fast portions
            base_percents = [40, 30, 20, 10][:fast_count]

            # Normalize if needed
            if sum(base_percents) != 100:
                factor = 100 / sum(base_percents)
                base_percents = [p * factor for p in base_percents]

            # Add variation
            remaining = fast_total
            for i, percent in enumerate(base_percents):
                if i == len(base_percents) - 1:
                    size = remaining
                else:
                    # Add ±10% variation
                    variation = random.uniform(-10, 10)
                    actual_percent = percent + variation
                    size = int(fast_total * actual_percent / 100)
                    remaining -= size

                portions.append(size)

        # Generate slow portions
        if slow_count > 0:
            # Equal distribution for slow portions
            base_size = slow_total // slow_count
            remaining = slow_total

            for i in range(slow_count):
                if i == slow_count - 1:
                    size = remaining
                else:
                    # Add ±15% variation
                    variation = random.uniform(-15, 15)
                    size = int(base_size * (1 + variation / 100))
                    remaining -= size

                portions.append(max(1, size))

        return portions

    def _generate_seed(
            self,
            channel_id: int,
            post_id: int,
            service_type: str
    ) -> str:
        """Generate deterministic seed for randomization"""
        data = f"{channel_id}_{post_id}_{service_type}_{datetime.now().date()}"
        return hashlib.md5(data.encode()).hexdigest()

    def _generate_unique_value(
            self,
            min_value: int,
            max_value: int,
            cache_key: str,
            seed: str
    ) -> int:
        """Generate value that's different from recent values"""

        # Get cached values
        if cache_key not in self.last_values_cache:
            self.last_values_cache[cache_key] = []

        cached = self.last_values_cache[cache_key]

        # Try to generate unique value
        random.seed(seed)
        attempts = 0
        max_attempts = 50

        while attempts < max_attempts:
            value = random.randint(min_value, max_value)

            # Check if value is different enough from recent ones
            if not cached or all(abs(value - v) > (max_value - min_value) * 0.1 for v in cached):
                # Add to cache
                cached.append(value)

                # Limit cache size
                if len(cached) > self.cache_size:
                    cached.pop(0)

                # Reset random seed
                random.seed()
                return value

            attempts += 1

        # Fallback: return any value if can't find unique
        random.seed()
        return random.randint(min_value, max_value)

    def should_skip_randomization(
            self,
            service_type: ServiceType,
            is_test_mode: bool = False
    ) -> bool:
        """Check if randomization should be skipped"""

        # Never randomize views
        if service_type == ServiceType.VIEWS:
            return True

        # Skip in test mode if configured
        if is_test_mode:
            return True

        return False

    def get_randomization_stats(self, channel_id: int) -> Dict[str, any]:
        """Get randomization statistics for channel"""
        stats = {
            "cached_values": {},
            "cache_size": self.cache_size
        }

        # Get cached values for this channel
        for key, values in self.last_values_cache.items():
            if str(channel_id) in key:
                service_type = key.split("_")[-1]
                stats["cached_values"][service_type] = values

        return stats

    def clear_cache(self, channel_id: Optional[int] = None):
        """Clear randomization cache"""
        if channel_id:
            # Clear specific channel
            keys_to_remove = [
                key for key in self.last_values_cache.keys()
                if str(channel_id) in key
            ]
            for key in keys_to_remove:
                del self.last_values_cache[key]

            self.log_info(f"Cleared cache for channel {channel_id}")
        else:
            # Clear all
            self.last_values_cache.clear()
            self.log_info("Cleared all randomization cache")


# Global randomizer instance
randomizer = Randomizer()