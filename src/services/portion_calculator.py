"""
Universal portion calculator for multi-order distribution
FIXED VERSION: Each portion = separate Nakrutka order
For reposts: only single portion allowed
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
    ),
    'single': DistributionStrategy(
        name='single',
        description='Single portion - for reposts',
        front_load_percent=100,
        fast_hours=1,
        total_hours=1,
        min_portions=1,
        max_portions=1
    )
}


@dataclass
class Portion:
    """Single portion configuration"""
    portion_number: int
    quantity_per_run: int
    runs: int
    interval_minutes: int
    scheduled_at: datetime
    total_quantity: int

    def validate(self, service: Service) -> Tuple[bool, Optional[str]]:
        """Validate portion parameters"""
        logger.debug(
            f"Validating portion {self.portion_number}",
            quantity_per_run=self.quantity_per_run,
            runs=self.runs,
            service_min=service.min_quantity,
            service_max=service.max_quantity
        )

        # Check quantity per run
        if self.quantity_per_run < service.min_quantity:
            return False, f"Quantity per run {self.quantity_per_run} < min {service.min_quantity}"

        if self.quantity_per_run > service.max_quantity:
            return False, f"Quantity per run {self.quantity_per_run} > max {service.max_quantity}"

        # Check total
        actual_total = self.quantity_per_run * self.runs
        if abs(actual_total - self.total_quantity) > self.runs:
            return False, f"Total mismatch: {actual_total} vs {self.total_quantity}"

        # Check runs
        if self.runs < 1:
            return False, "Runs must be >= 1"

        if self.runs > 1000:
            return False, "Runs must be <= 1000 (Nakrutka limit)"

        # Check interval
        if self.runs > 1 and self.interval_minutes < 5:
            return False, "Interval must be >= 5 minutes for drip-feed"

        return True, None


class OptimizedPortionCalculator:
    """
    Portion calculator - each portion creates separate Nakrutka order
    """

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
        force_single: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Calculate portions for creating multiple orders

        Args:
            total_quantity: Total quantity to distribute
            service: Service with limits
            start_time: When to start
            strategy: Distribution strategy
            force_single: Force single portion (for reposts)

        Returns:
            List of portion configurations
        """
        if not start_time:
            start_time = datetime.utcnow()

        logger.info(
            f"=== Calculating portions ===",
            total_quantity=total_quantity,
            service_id=service.nakrutka_id,
            service_name=service.service_name,
            force_single=force_single,
            strategy=strategy
        )

        # Force single portion for reposts or if requested
        if force_single or service.service_type == 'reposts':
            logger.info("Using single portion strategy")
            return self._create_single_portion(total_quantity, service, start_time)

        # Use templates or dynamic calculation
        if self.templates:
            logger.info(f"Using {len(self.templates)} templates")
            portions = self._calculate_from_templates(
                total_quantity, service, start_time
            )
        else:
            logger.info("Using dynamic calculation")
            portions = self._calculate_dynamic(
                total_quantity, service, start_time, strategy
            )

        # Validate all portions
        self._validate_portions(portions, service, total_quantity)

        logger.info(
            f"Calculated {len(portions)} portions",
            total_runs=sum(p['runs'] for p in portions),
            actual_total=sum(p['quantity_per_run'] * p['runs'] for p in portions)
        )

        return portions

    def _create_single_portion(
        self,
        quantity: int,
        service: Service,
        start_time: datetime
    ) -> List[Dict[str, Any]]:
        """Create single portion (for reposts)"""
        # Adjust quantity to service limits
        adjusted = max(service.min_quantity, min(quantity, service.max_quantity))

        if adjusted != quantity:
            logger.warning(
                f"Adjusted quantity for single portion",
                original=quantity,
                adjusted=adjusted,
                limits=(service.min_quantity, service.max_quantity)
            )

        return [{
            'portion_number': 1,
            'quantity_per_run': adjusted,
            'runs': 1,
            'interval_minutes': 0,
            'scheduled_at': start_time,
            'total_quantity': adjusted
        }]

    def _calculate_from_templates(
        self,
        total_quantity: int,
        service: Service,
        start_time: datetime
    ) -> List[Dict[str, Any]]:
        """Calculate portions from templates"""
        portions = []
        remaining = total_quantity
        current_time = start_time

        for i, template in enumerate(self.templates):
            # Calculate quantity for this portion
            if i == len(self.templates) - 1:
                # Last portion gets remaining
                portion_quantity = remaining
            else:
                portion_quantity = int(total_quantity * float(template.quantity_percent) / 100)
                portion_quantity = max(1, portion_quantity)

            if portion_quantity <= 0:
                continue

            logger.debug(
                f"Processing template {template.portion_number}",
                percent=float(template.quantity_percent),
                calculated_quantity=portion_quantity
            )

            # Parse runs formula
            quantity_per_run, runs = self._parse_runs_formula(
                template.runs_formula,
                portion_quantity,
                service
            )

            # Schedule time
            if template.start_delay_minutes > 0:
                scheduled_at = start_time + timedelta(minutes=template.start_delay_minutes)
            else:
                scheduled_at = current_time

            # Create portion
            portion = {
                'portion_number': template.portion_number,
                'quantity_per_run': quantity_per_run,
                'runs': runs,
                'interval_minutes': template.interval_minutes,
                'scheduled_at': scheduled_at,
                'total_quantity': quantity_per_run * runs,
                'template_percent': float(template.quantity_percent)
            }

            portions.append(portion)
            remaining -= portion['total_quantity']

            # Update current time for next portion
            if runs > 1:
                duration = (runs - 1) * template.interval_minutes
                current_time = scheduled_at + timedelta(minutes=duration)
            else:
                current_time = scheduled_at

            logger.debug(
                f"Created portion {template.portion_number}",
                quantity_per_run=quantity_per_run,
                runs=runs,
                total=quantity_per_run * runs
            )

        return portions

    def _calculate_dynamic(
        self,
        total_quantity: int,
        service: Service,
        start_time: datetime,
        strategy: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Calculate portions dynamically without templates"""
        if not strategy:
            strategy = self._auto_select_strategy(total_quantity, service)

        self._strategy = STRATEGIES.get(strategy, STRATEGIES['standard'])

        logger.info(f"Using {strategy} strategy for dynamic calculation")

        # Determine number of portions
        if total_quantity < 100:
            num_portions = 2
        elif total_quantity < 1000:
            num_portions = min(5, self._strategy.max_portions)
        elif total_quantity < 10000:
            num_portions = min(7, self._strategy.max_portions)
        else:
            num_portions = self._strategy.max_portions

        # Distribute quantities
        portions = []
        remaining = total_quantity

        # Use decreasing distribution
        for i in range(num_portions):
            if i == num_portions - 1:
                # Last portion gets remaining
                portion_quantity = remaining
            else:
                # Calculate based on strategy
                if i < num_portions // 2:
                    # Front-loaded portions
                    percent = self._strategy.front_load_percent / (num_portions // 2)
                else:
                    # Back-loaded portions
                    percent = (100 - self._strategy.front_load_percent) / (num_portions - num_portions // 2)

                portion_quantity = int(total_quantity * percent / 100)
                portion_quantity = max(service.min_quantity, portion_quantity)

            if portion_quantity <= 0:
                continue

            # Calculate runs and quantity per run
            optimal_runs = self._calculate_optimal_runs(portion_quantity, service)
            quantity_per_run = portion_quantity // optimal_runs

            # Adjust for service limits
            if quantity_per_run < service.min_quantity:
                quantity_per_run = service.min_quantity
                optimal_runs = portion_quantity // quantity_per_run
            elif quantity_per_run > service.max_quantity:
                quantity_per_run = service.max_quantity
                optimal_runs = math.ceil(portion_quantity / quantity_per_run)

            # Calculate interval based on strategy
            if i < num_portions // 2:
                interval = 15 + i * 3  # Increasing intervals for front portions
            else:
                interval = 30 + (i - num_portions // 2) * 10  # Longer intervals for back portions

            # Schedule time
            if i == 0:
                scheduled_at = start_time
            else:
                # Add some delay between portions
                scheduled_at = start_time + timedelta(minutes=i * 20)

            portion = {
                'portion_number': i + 1,
                'quantity_per_run': quantity_per_run,
                'runs': optimal_runs,
                'interval_minutes': interval,
                'scheduled_at': scheduled_at,
                'total_quantity': quantity_per_run * optimal_runs
            }

            portions.append(portion)
            remaining -= portion['total_quantity']

            logger.debug(
                f"Dynamic portion {i+1}",
                quantity=portion['total_quantity'],
                runs=optimal_runs,
                interval=interval
            )

        return portions

    def _parse_runs_formula(
        self,
        formula: str,
        portion_quantity: int,
        service: Service
    ) -> Tuple[int, int]:
        """Parse runs formula from template"""
        if formula.startswith('quantity/'):
            # Formula like "quantity/134"
            divisor = int(formula.replace('quantity/', ''))
            quantity_per_run = divisor
            runs = max(1, math.ceil(portion_quantity / divisor))

            # Adjust for service limits
            if quantity_per_run < service.min_quantity:
                quantity_per_run = service.min_quantity
                runs = max(1, portion_quantity // quantity_per_run)
            elif quantity_per_run > service.max_quantity:
                quantity_per_run = service.max_quantity
                runs = max(1, math.ceil(portion_quantity / quantity_per_run))
        else:
            # Fixed runs
            runs = max(1, int(formula) if formula.isdigit() else 1)
            quantity_per_run = max(1, portion_quantity // runs)

            # Adjust for service limits
            if quantity_per_run < service.min_quantity:
                quantity_per_run = service.min_quantity
                runs = max(1, portion_quantity // quantity_per_run)
            elif quantity_per_run > service.max_quantity:
                quantity_per_run = service.max_quantity
                runs = max(1, math.ceil(portion_quantity / quantity_per_run))

        return quantity_per_run, runs

    def _calculate_optimal_runs(
        self,
        quantity: int,
        service: Service
    ) -> int:
        """Calculate optimal number of runs for quantity"""
        # Target quantity per run (prefer middle of service range)
        target_per_run = (service.min_quantity + service.max_quantity) // 3

        # Calculate runs
        if quantity <= service.max_quantity:
            return 1  # Single run if fits

        runs = max(1, quantity // target_per_run)

        # Limit runs
        if runs > 50:
            runs = 50  # Reasonable limit

        return runs

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

    def _validate_portions(
        self,
        portions: List[Dict[str, Any]],
        service: Service,
        total_quantity: int
    ):
        """Validate all portions"""
        total_calculated = sum(p['total_quantity'] for p in portions)

        if abs(total_calculated - total_quantity) > len(portions):
            logger.warning(
                f"Total quantity mismatch",
                expected=total_quantity,
                calculated=total_calculated,
                difference=abs(total_calculated - total_quantity)
            )

        for portion in portions:
            p = Portion(
                portion_number=portion['portion_number'],
                quantity_per_run=portion['quantity_per_run'],
                runs=portion['runs'],
                interval_minutes=portion['interval_minutes'],
                scheduled_at=portion['scheduled_at'],
                total_quantity=portion['total_quantity']
            )

            valid, error = p.validate(service)
            if not valid:
                logger.error(
                    f"Invalid portion {portion['portion_number']}",
                    error=error
                )
                # Try to fix
                self._fix_invalid_portion(portion, service)

    def _fix_invalid_portion(
        self,
        portion: Dict[str, Any],
        service: Service
    ):
        """Try to fix invalid portion"""
        quantity_per_run = portion['quantity_per_run']
        runs = portion['runs']
        total = portion['total_quantity']

        # Fix quantity per run
        if quantity_per_run < service.min_quantity:
            quantity_per_run = service.min_quantity
        elif quantity_per_run > service.max_quantity:
            quantity_per_run = service.max_quantity

        # Recalculate runs
        runs = max(1, total // quantity_per_run)

        # Update portion
        portion['quantity_per_run'] = quantity_per_run
        portion['runs'] = runs
        portion['total_quantity'] = quantity_per_run * runs

        logger.info(
            f"Fixed portion {portion['portion_number']}",
            new_quantity_per_run=quantity_per_run,
            new_runs=runs
        )


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
        Each reaction type will get its own order!

        Args:
            total_quantity: Total reactions to distribute (already randomized if needed)
            randomized: Whether to apply additional randomization to distribution
            randomize_percent: Randomization percentage for distribution

        Returns:
            List of distributions with service details
        """
        logger.info(
            f"=== Distributing reactions ===",
            total_quantity=total_quantity,
            services=len(self.services),
            randomized=randomized
        )

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

                logger.debug(
                    f"Reaction distribution",
                    emoji=service.emoji,
                    service_id=service.service_id,
                    quantity=quantity,
                    proportion=f"{proportion*100:.1f}%"
                )

        # Log distribution stats
        logger.info(
            "Reaction distribution completed",
            total_quantity=total_quantity,
            distributions=len(distributions),
            remaining=remaining
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

    # Force single for reposts
    if service_type == 'reposts':
        return {
            'strategy': 'single',
            'portions': 1,
            'front_load_percent': 100,
            'notes': 'Single portion for reposts'
        }

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