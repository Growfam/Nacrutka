"""
Universal portion calculator for drip-feed distribution
"""
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_UP
import math

from src.database.models import PortionTemplate, Service, ChannelSettings
from src.utils.logger import get_logger
from src.utils.validators import DataValidator

logger = get_logger(__name__)


class PortionCalculator:
    """Calculates portions for orders based on templates"""

    def __init__(self, templates: List[PortionTemplate]):
        self.templates = sorted(templates, key=lambda x: x.portion_number)
        self._validate_templates()

    def _validate_templates(self):
        """Validate templates on initialization"""
        if not self.templates:
            raise ValueError("No templates provided")

        # Check total percentage
        total_percent = sum(float(t.quantity_percent) for t in self.templates)
        if abs(total_percent - 100.0) > 0.1:
            logger.warning(
                f"Template percentages sum to {total_percent}%, adjusting..."
            )

    def calculate_portions(
            self,
            total_quantity: int,
            service: Service,
            start_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Calculate portions based on templates

        Returns list of portions with:
        - quantity_per_run
        - runs
        - interval_minutes
        - scheduled_at
        - total_quantity
        """
        if not start_time:
            start_time = datetime.utcnow()

        portions = []
        remaining_quantity = total_quantity

        for i, template in enumerate(self.templates):
            # Calculate quantity for this portion
            if i == len(self.templates) - 1:
                # Last portion gets all remaining
                portion_quantity = remaining_quantity
            else:
                portion_quantity = int(
                    total_quantity * float(template.quantity_percent) / 100
                )

            if portion_quantity <= 0:
                continue

            # Calculate runs based on formula
            quantity_per_run, runs = self._calculate_runs(
                portion_quantity,
                template.runs_formula,
                service
            )

            # Adjust for service limits
            quantity_per_run, runs = self._adjust_for_limits(
                quantity_per_run,
                runs,
                service
            )

            # Calculate actual total for this portion
            actual_total = quantity_per_run * runs

            # Update remaining
            remaining_quantity -= actual_total

            # Calculate scheduled time
            scheduled_at = start_time + timedelta(
                minutes=template.start_delay_minutes
            )

            portions.append({
                'portion_number': template.portion_number,
                'quantity_per_run': quantity_per_run,
                'runs': runs,
                'interval_minutes': template.interval_minutes,
                'scheduled_at': scheduled_at,
                'total_quantity': actual_total,
                'template_percent': float(template.quantity_percent)
            })

        # Validate total
        total_calculated = sum(p['total_quantity'] for p in portions)
        if abs(total_calculated - total_quantity) > 5:
            logger.warning(
                f"Portion calculation mismatch: {total_calculated} vs {total_quantity}"
            )
            # Adjust last portion
            if portions:
                diff = total_quantity - total_calculated
                portions[-1]['total_quantity'] += diff
                portions[-1]['runs'] = math.ceil(
                    portions[-1]['total_quantity'] / portions[-1]['quantity_per_run']
                )

        return portions

    def _calculate_runs(
            self,
            portion_quantity: int,
            runs_formula: str,
            service: Service
    ) -> Tuple[int, int]:
        """Calculate quantity per run and number of runs"""
        if runs_formula.startswith('quantity/'):
            # Formula like "quantity/134"
            divisor = int(runs_formula.replace('quantity/', ''))
            quantity_per_run = divisor
            runs = math.ceil(portion_quantity / divisor)
        else:
            # Fixed runs
            runs = int(runs_formula) if runs_formula.isdigit() else 1
            quantity_per_run = math.ceil(portion_quantity / runs)

        return quantity_per_run, runs

    def _adjust_for_limits(
            self,
            quantity_per_run: int,
            runs: int,
            service: Service
    ) -> Tuple[int, int]:
        """Adjust quantity and runs for service limits"""
        # Check minimum
        if quantity_per_run < service.min_quantity:
            quantity_per_run = service.min_quantity
            runs = math.ceil((quantity_per_run * runs) / service.min_quantity)

        # Check maximum per run
        if quantity_per_run > service.max_quantity:
            # Need to split into more runs
            total = quantity_per_run * runs
            quantity_per_run = service.max_quantity
            runs = math.ceil(total / quantity_per_run)

        return quantity_per_run, runs


class ReactionDistributor:
    """Distributes reactions across different services"""

    def __init__(self, reaction_services: List[Dict[str, Any]]):
        self.reaction_services = reaction_services
        self._validate_services()

    def _validate_services(self):
        """Validate reaction services"""
        if not self.reaction_services:
            raise ValueError("No reaction services provided")

        total = sum(s.get('target_quantity', 0) for s in self.reaction_services)
        if total <= 0:
            raise ValueError("Total reaction quantity is 0")

    def distribute_reactions(
            self,
            total_quantity: int,
            randomized: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Distribute total reactions across services proportionally

        Returns list of:
        - service_id
        - quantity
        - emoji
        - proportion
        """
        # Calculate total target
        total_target = sum(s['target_quantity'] for s in self.reaction_services)

        distributions = []
        remaining = total_quantity

        for i, service in enumerate(self.reaction_services):
            # Calculate proportion
            proportion = service['target_quantity'] / total_target

            if i == len(self.reaction_services) - 1:
                # Last service gets all remaining
                quantity = remaining
            else:
                quantity = int(total_quantity * proportion)

            if quantity > 0:
                distributions.append({
                    'service_id': service['service_id'],
                    'quantity': quantity,
                    'emoji': service.get('emoji', 'unknown'),
                    'proportion': proportion,
                    'target_quantity': service['target_quantity']
                })

                remaining -= quantity

        return distributions


class DynamicPortionAdjuster:
    """Adjusts portions dynamically based on performance"""

    def __init__(self):
        self.performance_history = {}

    def adjust_portions(
            self,
            original_portions: List[Dict[str, Any]],
            channel_id: int,
            service_type: str
    ) -> List[Dict[str, Any]]:
        """Adjust portions based on historical performance"""
        # For now, return original portions
        # In future, can implement ML-based adjustments
        return original_portions

    def record_performance(
            self,
            channel_id: int,
            service_type: str,
            portions: List[Dict[str, Any]],
            completion_times: List[datetime]
    ):
        """Record performance for future adjustments"""
        key = f"{channel_id}_{service_type}"

        if key not in self.performance_history:
            self.performance_history[key] = []

        self.performance_history[key].append({
            'portions': portions,
            'completion_times': completion_times,
            'recorded_at': datetime.utcnow()
        })

        # Keep only last 100 records
        if len(self.performance_history[key]) > 100:
            self.performance_history[key] = self.performance_history[key][-100:]


def calculate_optimal_distribution(
        total_quantity: int,
        target_duration_hours: int = 24,
        front_load_percent: int = 70
) -> List[Dict[str, Any]]:
    """
    Calculate optimal portion distribution without templates

    Args:
        total_quantity: Total to distribute
        target_duration_hours: Target completion time
        front_load_percent: Percentage to deliver in first period

    Returns:
        List of portion configurations
    """
    portions = []

    # Calculate quantities
    fast_quantity = int(total_quantity * front_load_percent / 100)
    slow_quantity = total_quantity - fast_quantity

    # Fast portions (4 portions over 3-5 hours)
    fast_portions = [
        {'percent': 35, 'interval': 15},  # 35% of fast
        {'percent': 27, 'interval': 18},  # 27% of fast
        {'percent': 22, 'interval': 22},  # 22% of fast
        {'percent': 16, 'interval': 26},  # 16% of fast
    ]

    for i, config in enumerate(fast_portions):
        quantity = int(fast_quantity * config['percent'] / 100)
        if quantity > 0:
            # Calculate optimal runs
            if quantity > 500:
                runs = min(20, quantity // 100)
                quantity_per_run = quantity // runs
            else:
                runs = min(5, max(1, quantity // 50))
                quantity_per_run = quantity // runs

            portions.append({
                'portion_number': i + 1,
                'quantity_per_run': quantity_per_run,
                'runs': runs,
                'interval_minutes': config['interval'],
                'delay_minutes': 0,
                'total_quantity': quantity_per_run * runs
            })

    # Slow portion (remaining over rest of time)
    if slow_quantity > 0:
        # Calculate delay (after fast portions complete)
        fast_duration_minutes = sum(
            p['interval_minutes'] * (p['runs'] - 1)
            for p in portions
        )
        delay_minutes = max(180, fast_duration_minutes)  # At least 3 hours

        # Calculate runs for slow portion
        remaining_hours = target_duration_hours - (delay_minutes / 60)
        slow_runs = max(1, int(remaining_hours * 2))  # 2 runs per hour
        quantity_per_run = max(1, slow_quantity // slow_runs)

        portions.append({
            'portion_number': len(portions) + 1,
            'quantity_per_run': quantity_per_run,
            'runs': slow_runs,
            'interval_minutes': max(15, int(remaining_hours * 60 / slow_runs)),
            'delay_minutes': delay_minutes,
            'total_quantity': quantity_per_run * slow_runs
        })

    return portions


def validate_and_fix_portions(
        portions: List[Dict[str, Any]],
        total_target: int,
        service: Service
) -> List[Dict[str, Any]]:
    """Validate portions and fix if needed"""
    # Calculate total
    total_calculated = sum(p['total_quantity'] for p in portions)

    # Check if adjustment needed
    if abs(total_calculated - total_target) > 5:
        logger.info(
            f"Adjusting portions: {total_calculated} -> {total_target}"
        )

        # Adjust proportionally
        factor = total_target / total_calculated

        for portion in portions:
            old_total = portion['total_quantity']
            new_total = int(old_total * factor)

            # Recalculate runs
            portion['total_quantity'] = new_total
            portion['runs'] = math.ceil(new_total / portion['quantity_per_run'])

            # Ensure within limits
            if portion['quantity_per_run'] < service.min_quantity:
                portion['quantity_per_run'] = service.min_quantity
                portion['runs'] = math.ceil(new_total / service.min_quantity)

    # Validate each portion
    for portion in portions:
        # Ensure minimum quantity
        if portion['quantity_per_run'] < service.min_quantity:
            portion['quantity_per_run'] = service.min_quantity

        # Ensure maximum quantity
        if portion['quantity_per_run'] > service.max_quantity:
            portion['quantity_per_run'] = service.max_quantity
            portion['runs'] = math.ceil(
                portion['total_quantity'] / service.max_quantity
            )

    return portions