"""
Portion calculator for natural distribution
"""
import random
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

from src.database.models import ChannelSettings, ServiceType
from src.config import FAST_PORTIONS, SLOW_PORTIONS
from src.utils.logger import get_logger, LoggerMixin

logger = get_logger(__name__)


@dataclass
class Portion:
    """Single portion of orders"""
    number: int
    quantity: int
    percentage: float
    scheduled_at: datetime
    is_fast: bool

    # For drip-feed
    drops: Optional[int] = None  # quantity per run
    runs: Optional[int] = None  # number of runs
    interval: Optional[int] = None  # minutes between runs


class PortionCalculator(LoggerMixin):
    """Calculate portions for natural distribution"""

    def calculate_portions(
            self,
            total_quantity: int,
            settings: ChannelSettings,
            service_type: ServiceType,
            start_time: datetime = None
    ) -> List[Portion]:
        """Calculate portions based on service type and settings"""

        if start_time is None:
            start_time = datetime.now()

        # Apply randomization if needed
        actual_quantity = self._apply_randomization(
            total_quantity,
            settings.randomization_percent,
            service_type
        )

        self.log_debug(
            "Calculating portions",
            service_type=service_type,
            original_quantity=total_quantity,
            actual_quantity=actual_quantity,
            randomization=settings.randomization_percent
        )

        # Calculate based on service type
        if service_type == ServiceType.VIEWS:
            portions = self._calculate_view_portions(
                actual_quantity,
                settings,
                start_time
            )
        elif service_type == ServiceType.REACTIONS:
            portions = self._calculate_reaction_portions(
                actual_quantity,
                settings,
                start_time
            )
        elif service_type == ServiceType.REPOSTS:
            portions = self._calculate_repost_portions(
                actual_quantity,
                settings,
                start_time
            )
        else:
            raise ValueError(f"Unknown service type: {service_type}")

        self.log_info(
            "Portions calculated",
            service_type=service_type,
            total_quantity=actual_quantity,
            portions_count=len(portions)
        )

        return portions

    def _apply_randomization(
            self,
            quantity: int,
            randomization_percent: int,
            service_type: ServiceType
    ) -> int:
        """Apply randomization to quantity"""

        # Views don't get randomized
        if service_type == ServiceType.VIEWS or randomization_percent == 0:
            return quantity

        # Calculate range
        min_value = int(quantity * (1 - randomization_percent / 100))
        max_value = int(quantity * (1 + randomization_percent / 100))

        # Generate random value
        randomized = random.randint(min_value, max_value)

        self.log_debug(
            "Randomization applied",
            original=quantity,
            randomized=randomized,
            percent=randomization_percent
        )

        return randomized

    def _calculate_view_portions(
            self,
            quantity: int,
            settings: ChannelSettings,
            start_time: datetime
    ) -> List[Portion]:
        """Calculate portions for views (70/30 strategy)"""

        portions = []
        remaining = quantity

        # Get distribution percentages
        fast_percents = FAST_PORTIONS[:settings.portions_count - 1] if settings.portions_count > 1 else [100]
        slow_percents = SLOW_PORTIONS if settings.portions_count > len(fast_percents) else []

        # Calculate fast portions (immediate start)
        for i, percent in enumerate(fast_percents):
            portion_size = int(quantity * percent / 100)
            if i == len(fast_percents) - 1 and not slow_percents:
                # Last portion takes remaining
                portion_size = remaining

            portions.append(Portion(
                number=i + 1,
                quantity=portion_size,
                percentage=percent,
                scheduled_at=start_time,  # All fast portions start immediately
                is_fast=True
            ))

            remaining -= portion_size

        # Calculate slow portions (delayed start)
        slow_start = start_time + timedelta(hours=random.uniform(2, 3))

        for i, percent in enumerate(slow_percents):
            portion_size = remaining if i == len(slow_percents) - 1 else int(quantity * percent / 100)

            # Add random delay between slow portions
            delay_hours = random.uniform(3, 6) * i

            portions.append(Portion(
                number=len(fast_percents) + i + 1,
                quantity=portion_size,
                percentage=percent,
                scheduled_at=slow_start + timedelta(hours=delay_hours),
                is_fast=False
            ))

            remaining -= portion_size

        return portions

    def _calculate_reaction_portions(
            self,
            quantity: int,
            settings: ChannelSettings,
            start_time: datetime
    ) -> List[Portion]:
        """Calculate portions for reactions (single portion with drip-feed)"""

        # Reactions are delivered in one portion but with drip-feed
        drops = settings.drops_per_run
        runs = settings.calculate_runs(quantity)

        portion = Portion(
            number=1,
            quantity=quantity,
            percentage=100,
            scheduled_at=start_time,  # Start immediately
            is_fast=True,
            drops=drops,
            runs=runs,
            interval=settings.run_interval
        )

        self.log_debug(
            "Reaction portion calculated",
            quantity=quantity,
            drops=drops,
            runs=runs,
            interval=settings.run_interval
        )

        return [portion]

    def _calculate_repost_portions(
            self,
            quantity: int,
            settings: ChannelSettings,
            start_time: datetime
    ) -> List[Portion]:
        """Calculate portions for reposts (single delayed portion)"""

        # Reposts have delay and drip-feed
        drops = settings.drops_per_run
        runs = settings.calculate_runs(quantity)

        portion = Portion(
            number=1,
            quantity=quantity,
            percentage=100,
            scheduled_at=start_time + timedelta(minutes=settings.repost_delay_minutes),
            is_fast=False,
            drops=drops,
            runs=runs,
            interval=settings.run_interval
        )

        self.log_debug(
            "Repost portion calculated",
            quantity=quantity,
            delay_minutes=settings.repost_delay_minutes,
            drops=drops,
            runs=runs
        )

        return [portion]

    def distribute_reactions(
            self,
            total_quantity: int,
            distribution: Dict[str, int]
    ) -> Dict[str, int]:
        """Distribute total quantity among reaction types"""

        if not distribution:
            return {}

        result = {}
        remaining = total_quantity

        # Sort by percentage to handle rounding
        sorted_reactions = sorted(
            distribution.items(),
            key=lambda x: x[1],
            reverse=True
        )

        # Calculate quantities
        for i, (emoji, percentage) in enumerate(sorted_reactions):
            if i == len(sorted_reactions) - 1:
                # Last reaction gets remaining
                quantity = remaining
            else:
                quantity = int(total_quantity * percentage / 100)

            result[emoji] = quantity
            remaining -= quantity

        self.log_debug(
            "Reactions distributed",
            total=total_quantity,
            distribution=result
        )

        return result

    def validate_portion_sum(self, portions: List[Portion], expected_total: int) -> bool:
        """Validate that portions sum to expected total"""

        total = sum(p.quantity for p in portions)
        is_valid = total == expected_total

        if not is_valid:
            self.log_warning(
                "Portion sum mismatch",
                expected=expected_total,
                actual=total,
                difference=total - expected_total
            )

        return is_valid


# Global calculator instance
portion_calculator = PortionCalculator()