"""
Universal portion calculator for drip-feed distribution - FIXED VERSION
Порції використовуються тільки для внутрішнього трекінгу.
Nakrutka отримує ОДНЕ замовлення з загальними параметрами drip-feed.
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


@dataclass
class DripFeedParams:
    """Параметри для Nakrutka drip-feed API"""
    total_quantity: int      # Загальна кількість
    quantity_per_run: int    # Кількість на один run
    runs: int               # Кількість runs
    interval: int           # Інтервал між runs в хвилинах

    def validate(self, service: Service) -> Tuple[bool, Optional[str]]:
        """Перевірити параметри для сервісу"""
        # Перевірка quantity_per_run
        if self.quantity_per_run < service.min_quantity:
            return False, f"Quantity per run {self.quantity_per_run} < min {service.min_quantity}"

        if self.quantity_per_run > service.max_quantity:
            return False, f"Quantity per run {self.quantity_per_run} > max {service.max_quantity}"

        # Перевірка загальної кількості
        actual_total = self.quantity_per_run * self.runs
        if abs(actual_total - self.total_quantity) > self.runs:  # Допустима похибка
            return False, f"Total mismatch: {actual_total} vs {self.total_quantity}"

        # Перевірка runs
        if self.runs < 1:
            return False, "Runs must be >= 1"

        if self.runs > 1000:  # Nakrutka limit
            return False, "Runs must be <= 1000"

        # Перевірка інтервалу
        if self.runs > 1 and self.interval < 5:
            return False, "Interval must be >= 5 minutes for drip-feed"

        return True, None


class OptimizedPortionCalculator:
    """
    Enhanced portion calculator that creates proper drip-feed parameters
    ВАЖЛИВО: Порції - це лише для внутрішнього трекінгу прогресу!
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

    def calculate_drip_feed_params(
        self,
        total_quantity: int,
        service: Service,
        strategy: Optional[str] = None
    ) -> DripFeedParams:
        """
        Розрахувати параметри для ОДНОГО drip-feed замовлення Nakrutka

        Returns:
            DripFeedParams для відправки в Nakrutka API
        """
        if not strategy:
            strategy = self._auto_select_strategy(total_quantity, service)

        self._strategy = STRATEGIES.get(strategy, STRATEGIES['standard'])

        # Визначаємо оптимальну кількість runs
        if total_quantity < 500:
            # Малі замовлення - менше runs
            target_runs = 10
        elif total_quantity < 5000:
            # Середні замовлення
            target_runs = 30
        elif total_quantity < 50000:
            # Великі замовлення
            target_runs = 100
        else:
            # Дуже великі замовлення
            target_runs = 200

        # Розраховуємо quantity_per_run
        quantity_per_run = max(
            service.min_quantity,
            total_quantity // target_runs
        )

        # Перевіряємо максимум
        if quantity_per_run > service.max_quantity:
            quantity_per_run = service.max_quantity

        # Перераховуємо runs
        runs = math.ceil(total_quantity / quantity_per_run)

        # Обмеження Nakrutka
        if runs > 1000:
            runs = 1000
            quantity_per_run = math.ceil(total_quantity / runs)

        # Розраховуємо інтервал
        # Загальний час виконання в хвилинах
        total_minutes = self._strategy.total_hours * 60

        # Інтервал = загальний час / кількість runs
        interval = max(5, total_minutes // runs) if runs > 1 else 0

        # Створюємо параметри
        params = DripFeedParams(
            total_quantity=total_quantity,
            quantity_per_run=quantity_per_run,
            runs=runs,
            interval=interval
        )

        # Валідація
        valid, error = params.validate(service)
        if not valid:
            logger.warning(f"Initial params invalid: {error}, adjusting...")
            # Спробуємо виправити
            params = self._fix_drip_feed_params(params, service)

        logger.info(
            f"Calculated drip-feed params",
            total=total_quantity,
            per_run=params.quantity_per_run,
            runs=params.runs,
            interval=params.interval,
            strategy=strategy
        )

        return params

    def calculate_portions(
        self,
        total_quantity: int,
        service: Service,
        start_time: Optional[datetime] = None,
        strategy: Optional[str] = None,
        force_portions: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Calculate portions for INTERNAL TRACKING ONLY
        Порції НЕ відправляються в Nakrutka як окремі замовлення!

        Returns:
            List of portions for database tracking
        """
        if not start_time:
            start_time = datetime.utcnow()

        # Отримуємо параметри drip-feed
        drip_params = self.calculate_drip_feed_params(total_quantity, service, strategy)

        # Тепер створюємо порції для внутрішнього трекінгу
        # Вони відображають як буде розподілятися накрутка в часі
        if self.templates and not force_portions:
            portions = self._calculate_tracking_portions_from_templates(
                drip_params, start_time
            )
        else:
            portions = self._calculate_tracking_portions_dynamic(
                drip_params, start_time, force_portions
            )

        # Додаємо drip-feed параметри до кожної порції
        for portion in portions:
            portion['drip_feed_params'] = {
                'quantity_per_run': drip_params.quantity_per_run,
                'runs': drip_params.runs,
                'interval': drip_params.interval
            }

        return portions

    def _calculate_tracking_portions_from_templates(
        self,
        drip_params: DripFeedParams,
        start_time: datetime
    ) -> List[Dict[str, Any]]:
        """Створити порції для трекінгу на основі шаблонів"""
        portions = []
        total_runs = drip_params.runs

        # Розподіляємо runs між порціями відповідно до шаблонів
        remaining_runs = total_runs

        for i, template in enumerate(self.templates):
            if i == len(self.templates) - 1:
                # Остання порція отримує всі runs що залишилися
                portion_runs = remaining_runs
            else:
                # Розподіляємо пропорційно
                portion_runs = int(total_runs * float(template.quantity_percent) / 100)
                portion_runs = max(1, portion_runs)

            if portion_runs <= 0:
                continue

            # Кількість для цієї порції
            portion_quantity = portion_runs * drip_params.quantity_per_run

            # Час початку цієї порції
            # Розраховуємо коли почнеться перший run цієї порції
            runs_before = total_runs - remaining_runs
            start_delay = runs_before * drip_params.interval

            portions.append({
                'portion_number': template.portion_number,
                'quantity_per_run': drip_params.quantity_per_run,
                'runs': portion_runs,
                'interval_minutes': drip_params.interval,
                'scheduled_at': start_time + timedelta(minutes=start_delay),
                'total_quantity': portion_quantity,
                'template_percent': float(template.quantity_percent),
                'tracking_info': {
                    'first_run_index': runs_before + 1,
                    'last_run_index': runs_before + portion_runs,
                    'duration_minutes': (portion_runs - 1) * drip_params.interval
                }
            })

            remaining_runs -= portion_runs

        return portions

    def _calculate_tracking_portions_dynamic(
        self,
        drip_params: DripFeedParams,
        start_time: datetime,
        force_portions: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Створити порції для трекінгу динамічно"""
        num_portions = force_portions or 5  # За замовчуванням 5 порцій
        portions = []

        # Розподіляємо runs між порціями
        runs_per_portion = drip_params.runs // num_portions
        extra_runs = drip_params.runs % num_portions

        current_run_index = 0

        for i in range(num_portions):
            # Додаємо extra runs до перших порцій
            portion_runs = runs_per_portion + (1 if i < extra_runs else 0)

            if portion_runs <= 0:
                continue

            # Час початку цієї порції
            start_delay = current_run_index * drip_params.interval

            portions.append({
                'portion_number': i + 1,
                'quantity_per_run': drip_params.quantity_per_run,
                'runs': portion_runs,
                'interval_minutes': drip_params.interval,
                'scheduled_at': start_time + timedelta(minutes=start_delay),
                'total_quantity': portion_runs * drip_params.quantity_per_run,
                'tracking_info': {
                    'first_run_index': current_run_index + 1,
                    'last_run_index': current_run_index + portion_runs,
                    'duration_minutes': (portion_runs - 1) * drip_params.interval
                }
            })

            current_run_index += portion_runs

        return portions

    def _fix_drip_feed_params(
        self,
        params: DripFeedParams,
        service: Service
    ) -> DripFeedParams:
        """Спробувати виправити невалідні параметри"""
        # Якщо quantity_per_run занадто малий
        if params.quantity_per_run < service.min_quantity:
            params.quantity_per_run = service.min_quantity
            params.runs = math.ceil(params.total_quantity / params.quantity_per_run)

        # Якщо quantity_per_run занадто великий
        if params.quantity_per_run > service.max_quantity:
            params.quantity_per_run = service.max_quantity
            params.runs = math.ceil(params.total_quantity / params.quantity_per_run)

        # Якщо забагато runs
        if params.runs > 1000:
            params.runs = 1000
            params.quantity_per_run = math.ceil(params.total_quantity / params.runs)

            # Перевіряємо чи вкладаємося в ліміти
            if params.quantity_per_run > service.max_quantity:
                # Доведеться зменшити total_quantity
                params.quantity_per_run = service.max_quantity
                params.total_quantity = params.quantity_per_run * params.runs
                logger.warning(
                    f"Had to reduce total quantity from {params.total_quantity} "
                    f"to {params.quantity_per_run * params.runs} due to limits"
                )

        # Фіксимо інтервал
        if params.runs > 1 and params.interval < 5:
            params.interval = 5

        return params

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

    def get_simple_order_params(
        self,
        quantity: int,
        service: Service
    ) -> Dict[str, Any]:
        """
        Для простих замовлень без drip-feed (одноразова накрутка)
        """
        # Перевіряємо ліміти
        if quantity < service.min_quantity:
            quantity = service.min_quantity
        elif quantity > service.max_quantity:
            quantity = service.max_quantity

        return {
            'quantity': quantity,
            'drip_feed': False
        }


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
        ДЛЯ КОЖНОЇ РЕАКЦІЇ БУДЕ ОКРЕМЕ ЗАМОВЛЕННЯ В NAKRUTKA!

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