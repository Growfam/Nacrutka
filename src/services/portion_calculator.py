"""
Universal portion calculator for drip-feed distribution - OPTIMIZED VERSION
"""
from typing import List, Dict, Any, Tuple, Optional, Union
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_UP, ROUND_DOWN
from dataclasses import dataclass
import math
import json

from src.database.models import PortionTemplate, Service, ChannelSettings, ChannelReactionService
from src.utils.logger import get_logger
from src.utils.validators import DataValidator

logger = get_logger(__name__)


@dataclass
class DistributionStrategy:
    """Strategy for portion distribution"""
    name: str
    description: str
    front_load_percent: int  # % to deliver quickly
    fast_hours: int  # Hours for fast delivery
    total_hours: int  # Total delivery time
    min_portions: int
    max_portions: int


# Predefined strategies
STRATEGIES = {
    'aggressive': DistributionStrategy(
        name='aggressive',
        description='80% in 2-3 hours, 20% over day',
        front_load_percent=80,
        fast_hours=3,
        total_hours=24,
        min_portions=4,
        max_portions=6
    ),
    'standard': DistributionStrategy(
        name='standard',
        description='70% in 3-5 hours, 30% over day',
        front_load_percent=70,
        fast_hours=5,
        total_hours=24,
        min_portions=5,
        max_portions=7
    ),
    'smooth': DistributionStrategy(
        name='smooth',
        description='60% in 6-8 hours, 40% over day',
        front_load_percent=60,
        fast_hours=8,
        total_hours=24,
        min_portions=6,
        max_portions=10
    ),
    'natural': DistributionStrategy(
        name='natural',
        description='50% in 12 hours, 50% over day',
        front_load_percent=50,
        fast_hours=12,
        total_hours=24,
        min_portions=8,
        max_portions=12
    )
}


class OptimizedPortionCalculator:
    """Enhanced portion calculator with adaptive strategies"""

    def __init__(self, templates: Optional[List[PortionTemplate]] = None):
        self.templates = sorted(templates, key=lambda x: x.portion_number) if templates else None
        self._strategy = STRATEGIES['standard']  # Default strategy

        if self.templates:
            self._validate_and_fix_templates()

    def _validate_and_fix_templates(self):
        """Validate and auto-fix templates"""
        if not self.templates:
            return

        # Calculate total percentage
        total_percent = sum(float(t.quantity_percent) for t in self.templates)

        if abs(total_percent - 100.0) > 0.1:
            logger.warning(
                f"Template percentages sum to {total_percent}%, auto-adjusting..."
            )

            # Adjust percentages proportionally
            factor = Decimal('100') / Decimal(str(total_percent))
            for template in self.templates:
                old_percent = template.quantity_percent
                template.quantity_percent = Decimal(str(float(old_percent) * float(factor))).quantize(
                    Decimal('0.01'), rounding=ROUND_DOWN
                )

            # Fix rounding errors on last template
            new_total = sum(float(t.quantity_percent) for t in self.templates)
            if new_total != 100.0:
                diff = Decimal('100') - Decimal(str(new_total))
                self.templates[-1].quantity_percent += diff

    def calculate_portions(
        self,
        total_quantity: int,
        service: Service,
        start_time: Optional[datetime] = None,
        strategy: Optional[str] = None,
        force_portions: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Calculate portions with adaptive strategy

        Args:
            total_quantity: Total to distribute
            service: Service with limits
            start_time: When to start
            strategy: Strategy name or None for auto
            force_portions: Force specific number of portions

        Returns:
            List of optimized portions
        """
        if not start_time:
            start_time = datetime.utcnow()

        # Auto-select strategy if not provided
        if not strategy:
            strategy = self._auto_select_strategy(total_quantity, service)

        self._strategy = STRATEGIES.get(strategy, STRATEGIES['standard'])

        # Use templates if available, otherwise calculate dynamically
        if self.templates and not force_portions:
            return self._calculate_from_templates(total_quantity, service, start_time)
        else:
            return self._calculate_dynamic_portions(
                total_quantity, service, start_time, force_portions
            )

    def _auto_select_strategy(self, total_quantity: int, service: Service) -> str:
        """Auto-select best strategy based on quantity and service"""
        # For small quantities, use aggressive
        if total_quantity < 1000:
            return 'aggressive'

        # For large quantities with low limits, use smooth
        if total_quantity > 10000 and service.max_quantity < 1000:
            return 'smooth'

        # For very large quantities, use natural
        if total_quantity > 50000:
            return 'natural'

        # Default to standard
        return 'standard'

    def _calculate_from_templates(
        self,
        total_quantity: int,
        service: Service,
        start_time: datetime
    ) -> List[Dict[str, Any]]:
        """Calculate using existing templates"""
        portions = []
        remaining_quantity = total_quantity
        accumulated_delay = 0

        for i, template in enumerate(self.templates):
            # Calculate quantity for this portion
            if i == len(self.templates) - 1:
                # Last portion gets all remaining
                portion_quantity = remaining_quantity
            else:
                # Calculate based on percentage
                portion_quantity = int(
                    total_quantity * float(template.quantity_percent) / 100
                )
                # Ensure we don't exceed remaining
                portion_quantity = min(portion_quantity, remaining_quantity)

            if portion_quantity <= 0:
                continue

            # Smart calculation of runs and quantity per run
            quantity_per_run, runs = self._optimize_runs(
                portion_quantity,
                template.runs_formula,
                service
            )

            # Validate and adjust
            quantity_per_run, runs = self._adjust_for_service_limits(
                quantity_per_run, runs, portion_quantity, service
            )

            # Calculate actual total
            actual_total = quantity_per_run * runs
            remaining_quantity -= actual_total

            # Calculate timing
            if template.start_delay_minutes > 0:
                scheduled_at = start_time + timedelta(minutes=template.start_delay_minutes)
            else:
                scheduled_at = start_time + timedelta(minutes=accumulated_delay)

            # Add to accumulated delay for next portions
            if i < len(self.templates) - 1:  # Not last portion
                portion_duration = template.interval_minutes * (runs - 1)
                accumulated_delay += portion_duration / 4  # Overlap portions

            portions.append({
                'portion_number': template.portion_number,
                'quantity_per_run': quantity_per_run,
                'runs': runs,
                'interval_minutes': template.interval_minutes,
                'scheduled_at': scheduled_at,
                'total_quantity': actual_total,
                'template_percent': float(template.quantity_percent),
                'strategy': self._strategy.name
            })

        return self._validate_and_fix_portions(portions, total_quantity, service)

    def _calculate_dynamic_portions(
        self,
        total_quantity: int,
        service: Service,
        start_time: datetime,
        force_portions: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Calculate portions dynamically without templates"""
        # Determine number of portions
        if force_portions:
            num_portions = force_portions
        else:
            num_portions = self._calculate_optimal_portions_count(total_quantity, service)

        portions = []

        # Split into fast and slow quantities
        fast_quantity = int(total_quantity * self._strategy.front_load_percent / 100)
        slow_quantity = total_quantity - fast_quantity

        # Calculate fast portions (all but last)
        fast_portions_count = num_portions - 1
        if fast_portions_count > 0:
            fast_portion_quantities = self._distribute_decreasing(
                fast_quantity, fast_portions_count
            )

            # Create fast portions
            for i, qty in enumerate(fast_portion_quantities):
                if qty <= 0:
                    continue

                # Calculate optimal runs
                quantity_per_run, runs = self._calculate_optimal_runs(qty, service)

                # Calculate interval (increasing)
                base_interval = 15
                interval = base_interval + (i * 3)  # 15, 18, 21, 24...

                portions.append({
                    'portion_number': i + 1,
                    'quantity_per_run': quantity_per_run,
                    'runs': runs,
                    'interval_minutes': interval,
                    'scheduled_at': start_time,  # All start immediately
                    'total_quantity': quantity_per_run * runs,
                    'strategy': self._strategy.name
                })

        # Add slow portion (last one)
        if slow_quantity > 0:
            # Calculate delay based on fast portions completion
            fast_completion_minutes = self._estimate_fast_completion_time(portions)
            delay_minutes = max(
                self._strategy.fast_hours * 60,
                fast_completion_minutes + 30
            )

            # Calculate runs for slow portion
            remaining_hours = self._strategy.total_hours - (delay_minutes / 60)
            optimal_runs = max(1, int(remaining_hours * 2))  # ~2 runs per hour

            quantity_per_run = max(1, slow_quantity // optimal_runs)
            runs = math.ceil(slow_quantity / quantity_per_run)

            # Adjust for service limits
            quantity_per_run, runs = self._adjust_for_service_limits(
                quantity_per_run, runs, slow_quantity, service
            )

            # Calculate interval
            interval = max(15, int((remaining_hours * 60) / runs))

            portions.append({
                'portion_number': len(portions) + 1,
                'quantity_per_run': quantity_per_run,
                'runs': runs,
                'interval_minutes': interval,
                'scheduled_at': start_time + timedelta(minutes=delay_minutes),
                'total_quantity': quantity_per_run * runs,
                'strategy': self._strategy.name
            })

        return self._validate_and_fix_portions(portions, total_quantity, service)

    def _optimize_runs(
        self,
        portion_quantity: int,
        runs_formula: str,
        service: Service
    ) -> Tuple[int, int]:
        """Smart calculation of runs and quantity per run"""
        if runs_formula.startswith('quantity/'):
            # Formula like "quantity/134"
            try:
                divisor = int(runs_formula.replace('quantity/', ''))
                # This is quantity per run, not divisor
                quantity_per_run = divisor
                runs = math.ceil(portion_quantity / quantity_per_run)

                # Optimize if too many runs
                if runs > 100:
                    # Recalculate for max 100 runs
                    runs = 100
                    quantity_per_run = math.ceil(portion_quantity / runs)

                return quantity_per_run, runs

            except (ValueError, ZeroDivisionError):
                logger.error(f"Invalid runs formula: {runs_formula}")
                # Fallback to optimal calculation
                return self._calculate_optimal_runs(portion_quantity, service)

        elif runs_formula.isdigit():
            # Fixed runs number
            runs = int(runs_formula)
            quantity_per_run = math.ceil(portion_quantity / runs)
            return quantity_per_run, runs

        else:
            # Unknown formula, calculate optimal
            return self._calculate_optimal_runs(portion_quantity, service)

    def _calculate_optimal_runs(
        self,
        total_quantity: int,
        service: Service
    ) -> Tuple[int, int]:
        """Calculate optimal runs and quantity per run"""
        # Target quantity per run based on service limits
        ideal_quantity_per_run = min(
            service.max_quantity // 2,  # Half of max for safety
            max(service.min_quantity * 10, 100)  # At least 10x minimum or 100
        )

        # Calculate runs
        runs = math.ceil(total_quantity / ideal_quantity_per_run)

        # Optimize runs count
        if runs > 100:
            runs = 100
            quantity_per_run = math.ceil(total_quantity / runs)
        elif runs < 1:
            runs = 1
            quantity_per_run = total_quantity
        else:
            quantity_per_run = math.ceil(total_quantity / runs)

        return quantity_per_run, runs

    def _adjust_for_service_limits(
        self,
        quantity_per_run: int,
        runs: int,
        target_total: int,
        service: Service
    ) -> Tuple[int, int]:
        """Adjust for service min/max limits"""
        # Check minimum
        if quantity_per_run < service.min_quantity:
            quantity_per_run = service.min_quantity
            runs = math.ceil(target_total / quantity_per_run)

            # If too many runs, increase quantity per run
            if runs > 100:
                quantity_per_run = math.ceil(target_total / 100)
                quantity_per_run = max(quantity_per_run, service.min_quantity)
                runs = math.ceil(target_total / quantity_per_run)

        # Check maximum
        if quantity_per_run > service.max_quantity:
            quantity_per_run = service.max_quantity
            runs = math.ceil(target_total / quantity_per_run)

        return quantity_per_run, runs

    def _calculate_optimal_portions_count(
        self,
        total_quantity: int,
        service: Service
    ) -> int:
        """Calculate optimal number of portions"""
        # Based on quantity
        if total_quantity < 500:
            return max(self._strategy.min_portions, 3)
        elif total_quantity < 5000:
            return max(self._strategy.min_portions, 5)
        elif total_quantity < 50000:
            return min(self._strategy.max_portions, 7)
        else:
            return self._strategy.max_portions

    def _distribute_decreasing(
        self,
        total: int,
        portions: int
    ) -> List[int]:
        """Distribute with decreasing amounts"""
        if portions <= 0:
            return []

        if portions == 1:
            return [total]

        # Use geometric series for smooth decrease
        # First portion gets ~35%, second ~25%, third ~20%, etc.
        ratios = []
        base_ratio = 0.35
        decay = 0.85

        for i in range(portions):
            ratio = base_ratio * (decay ** i)
            ratios.append(ratio)

        # Normalize ratios to sum to 1
        total_ratio = sum(ratios)
        ratios = [r / total_ratio for r in ratios]

        # Calculate quantities
        quantities = []
        remaining = total

        for i, ratio in enumerate(ratios):
            if i == len(ratios) - 1:  # Last portion
                qty = remaining
            else:
                qty = int(total * ratio)
                remaining -= qty
            quantities.append(qty)

        return quantities

    def _estimate_fast_completion_time(self, portions: List[Dict[str, Any]]) -> int:
        """Estimate when fast portions will complete (minutes)"""
        max_time = 0

        for portion in portions:
            runs = portion.get('runs', 1)
            interval = portion.get('interval_minutes', 0)
            total_time = interval * (runs - 1)

            if total_time > max_time:
                max_time = total_time

        return max_time

    def _validate_and_fix_portions(
        self,
        portions: List[Dict[str, Any]],
        total_target: int,
        service: Service
    ) -> List[Dict[str, Any]]:
        """Validate portions and fix if needed"""
        if not portions:
            return portions

        # Calculate actual total
        actual_total = sum(p['total_quantity'] for p in portions)

        # Check if adjustment needed (more than 1% deviation)
        deviation = abs(actual_total - total_target)
        if deviation > total_target * 0.01:
            logger.info(
                f"Adjusting portions: {actual_total} -> {total_target} "
                f"(deviation: {deviation})"
            )

            # Adjust last portion
            last_portion = portions[-1]
            adjustment = total_target - actual_total

            new_total = last_portion['total_quantity'] + adjustment
            if new_total > 0:
                # Recalculate runs for new total
                last_portion['total_quantity'] = new_total
                quantity_per_run = last_portion['quantity_per_run']

                # Ensure within limits
                if new_total < service.min_quantity:
                    quantity_per_run = service.min_quantity
                elif quantity_per_run > service.max_quantity:
                    quantity_per_run = service.max_quantity

                last_portion['runs'] = math.ceil(new_total / quantity_per_run)
                last_portion['quantity_per_run'] = math.ceil(new_total / last_portion['runs'])

                # Final adjustment for exact match
                last_portion['total_quantity'] = last_portion['quantity_per_run'] * last_portion['runs']

        # Ensure all portions are valid
        for portion in portions:
            # Check minimum quantity
            if portion['quantity_per_run'] < service.min_quantity:
                logger.warning(
                    f"Portion {portion['portion_number']} quantity "
                    f"{portion['quantity_per_run']} below minimum {service.min_quantity}"
                )
                portion['quantity_per_run'] = service.min_quantity

            # Check maximum quantity
            if portion['quantity_per_run'] > service.max_quantity:
                logger.warning(
                    f"Portion {portion['portion_number']} quantity "
                    f"{portion['quantity_per_run']} above maximum {service.max_quantity}"
                )
                portion['quantity_per_run'] = service.max_quantity

            # Ensure positive values
            portion['runs'] = max(1, portion['runs'])
            portion['interval_minutes'] = max(0, portion['interval_minutes'])

        return portions


class ReactionDistributor:
    """Enhanced reaction distributor with proper database integration"""

    def __init__(self, channel_reaction_services: List[ChannelReactionService]):
        """
        Initialize with channel_reaction_services from database

        Args:
            channel_reaction_services: List of ChannelReactionService objects
        """
        self.services = channel_reaction_services
        self._validate_services()

    def _validate_services(self):
        """Validate reaction services configuration"""
        if not self.services:
            raise ValueError("No reaction services provided")

        total = sum(s.target_quantity for s in self.services)
        if total <= 0:
            raise ValueError("Total reaction quantity must be positive")

        # Check for duplicate service IDs
        service_ids = [s.service_id for s in self.services]
        if len(service_ids) != len(set(service_ids)):
            logger.warning("Duplicate service IDs found in reaction configuration")

    def distribute_reactions(
        self,
        total_quantity: int,
        randomized: bool = False,
        randomize_percent: int = 40
    ) -> List[Dict[str, Any]]:
        """
        Distribute total reactions across services proportionally

        Args:
            total_quantity: Total reactions to distribute (already randomized if needed)
            randomized: Whether to apply additional randomization to distribution
            randomize_percent: Randomization percentage for distribution

        Returns:
            List of distributions with service details
        """
        # Calculate total target from configuration
        config_total = sum(s.target_quantity for s in self.services)

        distributions = []
        remaining = total_quantity

        # Sort services by target quantity (largest first) for better distribution
        sorted_services = sorted(
            self.services,
            key=lambda s: s.target_quantity,
            reverse=True
        )

        for i, service in enumerate(sorted_services):
            # Calculate base proportion
            proportion = service.target_quantity / config_total

            if i == len(sorted_services) - 1:
                # Last service gets all remaining
                quantity = remaining
            else:
                # Calculate quantity for this service
                base_quantity = int(total_quantity * proportion)

                # Apply distribution randomization if enabled
                if randomized and randomize_percent > 0:
                    # Randomize the distribution itself
                    min_qty = int(base_quantity * (1 - randomize_percent / 100))
                    max_qty = int(base_quantity * (1 + randomize_percent / 100))

                    import random
                    quantity = random.randint(min_qty, max_qty)
                else:
                    quantity = base_quantity

                # Ensure we don't exceed remaining
                quantity = min(quantity, remaining)

            if quantity > 0:
                distributions.append({
                    'service_id': service.service_id,
                    'quantity': quantity,
                    'emoji': service.emoji,
                    'proportion': proportion,
                    'target_quantity': service.target_quantity,
                    'actual_proportion': quantity / total_quantity if total_quantity > 0 else 0
                })

                remaining -= quantity

        # Log distribution stats
        logger.info(
            "Reaction distribution calculated",
            total_quantity=total_quantity,
            distributions=len(distributions),
            remaining=remaining,
            randomized=randomized
        )

        # Validate distribution
        total_distributed = sum(d['quantity'] for d in distributions)
        if abs(total_distributed - total_quantity) > 1:
            logger.error(
                f"Distribution mismatch: {total_distributed} != {total_quantity}"
            )

        return distributions

    def get_emoji_mapping(self) -> Dict[str, int]:
        """Get emoji to service_id mapping"""
        return {
            service.emoji: service.service_id
            for service in self.services
        }

    def get_service_distribution_stats(self) -> Dict[str, Any]:
        """Get statistics about the service distribution"""
        total = sum(s.target_quantity for s in self.services)

        return {
            'total_services': len(self.services),
            'total_target_quantity': total,
            'services': [
                {
                    'service_id': s.service_id,
                    'emoji': s.emoji,
                    'target_quantity': s.target_quantity,
                    'percentage': (s.target_quantity / total * 100) if total > 0 else 0
                }
                for s in self.services
            ]
        }


def calculate_adaptive_distribution(
    total_quantity: int,
    service_type: str,
    channel_id: int,
    historical_performance: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Calculate distribution based on historical performance and channel specifics

    Args:
        total_quantity: Total to distribute
        service_type: Type of service (views/reactions/reposts)
        channel_id: Channel ID for channel-specific optimization
        historical_performance: Past performance data

    Returns:
        Optimized distribution parameters
    """
    # Default strategy
    strategy = 'standard'

    # Analyze historical performance if available
    if historical_performance:
        avg_completion_time = historical_performance.get('avg_completion_hours', 24)
        success_rate = historical_performance.get('success_rate', 1.0)

        # Adjust strategy based on performance
        if success_rate < 0.8:
            # Poor success rate - use smoother distribution
            strategy = 'smooth'
        elif avg_completion_time < 12:
            # Fast completion - can be more aggressive
            strategy = 'aggressive'
        elif avg_completion_time > 36:
            # Slow completion - use natural distribution
            strategy = 'natural'

    # Service-specific adjustments
    if service_type == 'views':
        # Views can be more aggressive
        if strategy == 'standard':
            strategy = 'aggressive'
    elif service_type == 'reactions':
        # Reactions should be more natural
        if strategy == 'aggressive':
            strategy = 'standard'
    elif service_type == 'reposts':
        # Reposts should be smoothest
        if strategy in ['aggressive', 'standard']:
            strategy = 'smooth'

    # Quantity-based adjustments
    if total_quantity < 100:
        # Very small - just do it quickly
        return {
            'strategy': 'aggressive',
            'portions': 1,
            'front_load_percent': 100,
            'notes': 'Single portion for small quantity'
        }
    elif total_quantity > 100000:
        # Very large - ensure smooth distribution
        strategy = 'natural'

    return {
        'strategy': strategy,
        'portions': STRATEGIES[strategy].max_portions,
        'front_load_percent': STRATEGIES[strategy].front_load_percent,
        'fast_hours': STRATEGIES[strategy].fast_hours,
        'total_hours': STRATEGIES[strategy].total_hours
    }


# Backward compatibility
PortionCalculator = OptimizedPortionCalculator