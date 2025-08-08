"""
Dynamic optimizer for portion distribution and service selection
"""
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import asyncio
import statistics

from src.database.connection import DatabaseConnection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DynamicPortionOptimizer:
    """
    Intelligent optimizer that learns from historical data
    to improve portion distribution and service selection
    """

    def __init__(self):
        self.performance_history = defaultdict(list)
        self.service_scores = {}
        self.channel_preferences = defaultdict(dict)
        self._cache_ttl = 3600  # 1 hour

    def optimize_portions(
            self,
            portions: List[Dict[str, Any]],
            channel_id: int,
            service_type: str,
            historical_data: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Optimize portion distribution based on historical performance

        Args:
            portions: Original portions from templates
            channel_id: Channel ID for channel-specific optimization
            service_type: Type of service (views/reactions/reposts)
            historical_data: Past performance data

        Returns:
            Optimized portions
        """
        if not historical_data:
            historical_data = self._get_channel_history(channel_id, service_type)

        # Analyze performance patterns
        patterns = self._analyze_patterns(historical_data)

        # Apply optimizations
        optimized_portions = []

        for portion in portions:
            optimized = portion.copy()

            # Optimize timing
            if patterns.get('best_start_time'):
                optimized['scheduled_at'] = self._adjust_to_best_time(
                    optimized.get('scheduled_at', datetime.utcnow()),
                    patterns['best_start_time']
                )

            # Optimize quantity distribution
            if patterns.get('optimal_run_size'):
                optimized = self._optimize_run_size(optimized, patterns['optimal_run_size'])

            # Optimize intervals
            if patterns.get('optimal_interval'):
                optimized['interval_minutes'] = self._adjust_interval(
                    optimized['interval_minutes'],
                    patterns['optimal_interval']
                )

            optimized_portions.append(optimized)

        # Ensure total quantity is preserved
        self._validate_total_quantity(portions, optimized_portions)

        return optimized_portions

    def select_best_service(
            self,
            service_type: str,
            quantity: int,
            available_services: List[Dict[str, Any]],
            channel_id: Optional[int] = None
    ) -> Optional[int]:
        """
        Select best service based on historical performance

        Args:
            service_type: Type of service needed
            quantity: Quantity to order
            available_services: List of available services
            channel_id: Channel ID for channel-specific preferences

        Returns:
            Best service ID or None
        """
        if not available_services:
            return None

        # Score each service
        scored_services = []

        for service in available_services:
            service_id = service['nakrutka_id']

            # Base score from historical performance
            base_score = self._get_service_score(service_id)

            # Adjust for channel preferences
            if channel_id:
                channel_score = self._get_channel_service_preference(channel_id, service_id)
                base_score = base_score * 0.7 + channel_score * 0.3

            # Adjust for price (lower is better)
            price_score = 1.0 / (1.0 + float(service.get('price_per_1000', 0.1)))

            # Adjust for quantity fit
            quantity_score = self._calculate_quantity_fit_score(
                quantity,
                service.get('min_quantity', 1),
                service.get('max_quantity', 1000000)
            )

            # Combined score
            total_score = (
                    base_score * 0.5 +
                    price_score * 0.3 +
                    quantity_score * 0.2
            )

            scored_services.append((service_id, total_score))

        # Sort by score descending
        scored_services.sort(key=lambda x: x[1], reverse=True)

        # Return best service
        return scored_services[0][0] if scored_services else None

    def record_performance(
            self,
            channel_id: int,
            service_type: str,
            order_id: int,
            completion_time: float,
            success: bool = True,
            actual_quantity: Optional[int] = None,
            target_quantity: Optional[int] = None,
            service_id: Optional[int] = None,
            cost: Optional[float] = None
    ):
        """Record order performance for learning"""
        performance_data = {
            'channel_id': channel_id,
            'service_type': service_type,
            'order_id': order_id,
            'completion_time': completion_time,
            'success': success,
            'actual_quantity': actual_quantity,
            'target_quantity': target_quantity,
            'service_id': service_id,
            'cost': cost,
            'timestamp': datetime.utcnow()
        }

        # Store in history
        key = f"{channel_id}:{service_type}"
        self.performance_history[key].append(performance_data)

        # Keep only recent history (last 1000 records)
        if len(self.performance_history[key]) > 1000:
            self.performance_history[key] = self.performance_history[key][-1000:]

        # Update service score
        if service_id:
            self._update_service_score(service_id, success, completion_time)

        # Update channel preferences
        if service_id and channel_id:
            self._update_channel_preference(channel_id, service_id, success)

    def calculate_default_portions(
            self,
            quantity: int,
            service: Dict[str, Any],
            delay_minutes: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Calculate default portion distribution when no templates exist

        Args:
            quantity: Total quantity to distribute
            service: Service information with limits
            delay_minutes: Initial delay

        Returns:
            List of portion configurations
        """
        # Determine strategy based on quantity
        if quantity < 100:
            # Single portion for small quantities
            return [{
                'portion_number': 1,
                'quantity_per_run': quantity,
                'runs': 1,
                'interval_minutes': 0,
                'scheduled_at': datetime.utcnow() + timedelta(minutes=delay_minutes),
                'total_quantity': quantity
            }]

        # Calculate optimal distribution
        if quantity < 1000:
            num_portions = 3
        elif quantity < 10000:
            num_portions = 5
        else:
            num_portions = 7

        # Use 70/30 distribution strategy
        fast_percent = 70
        fast_quantity = int(quantity * fast_percent / 100)
        slow_quantity = quantity - fast_quantity

        portions = []

        # Fast portions (all but last)
        if num_portions > 1:
            fast_portions_count = num_portions - 1
            fast_quantities = self._distribute_quantity(fast_quantity, fast_portions_count)

            for i, qty in enumerate(fast_quantities):
                # Calculate runs and quantity per run
                optimal_run_size = min(
                    service.get('max_quantity', 1000) // 2,
                    max(service.get('min_quantity', 1) * 10, 100)
                )

                runs = max(1, qty // optimal_run_size)
                quantity_per_run = qty // runs

                # Adjust for service limits
                if quantity_per_run < service.get('min_quantity', 1):
                    quantity_per_run = service.get('min_quantity', 1)
                    runs = qty // quantity_per_run

                portions.append({
                    'portion_number': i + 1,
                    'quantity_per_run': quantity_per_run,
                    'runs': runs,
                    'interval_minutes': 15 + (i * 3),  # Increasing intervals
                    'scheduled_at': datetime.utcnow() + timedelta(minutes=delay_minutes),
                    'total_quantity': quantity_per_run * runs
                })

        # Slow portion (last one)
        if slow_quantity > 0:
            # Delay for slow portion
            slow_delay = max(180, delay_minutes + 180)  # At least 3 hours

            # Calculate runs for slow portion
            slow_runs = max(1, 24)  # Spread over day
            quantity_per_run = max(1, slow_quantity // slow_runs)

            # Adjust for service limits
            if quantity_per_run < service.get('min_quantity', 1):
                quantity_per_run = service.get('min_quantity', 1)
                slow_runs = slow_quantity // quantity_per_run

            portions.append({
                'portion_number': len(portions) + 1,
                'quantity_per_run': quantity_per_run,
                'runs': slow_runs,
                'interval_minutes': 60,  # 1 hour intervals
                'scheduled_at': datetime.utcnow() + timedelta(minutes=slow_delay),
                'total_quantity': quantity_per_run * slow_runs
            })

        return portions

    def get_stats(self) -> Dict[str, Any]:
        """Get optimizer statistics"""
        stats = {
            'total_records': sum(len(records) for records in self.performance_history.values()),
            'channels_tracked': len(set(
                record['channel_id']
                for records in self.performance_history.values()
                for record in records
            )),
            'service_scores': len(self.service_scores),
            'avg_completion_time': self._calculate_avg_completion_time(),
            'success_rate': self._calculate_overall_success_rate()
        }

        return stats

    # Private helper methods

    def _get_channel_history(self, channel_id: int, service_type: str) -> Dict[str, Any]:
        """Get historical data for channel"""
        key = f"{channel_id}:{service_type}"
        history = self.performance_history.get(key, [])

        if not history:
            return {}

        # Analyze recent history
        recent_history = [h for h in history if
                          (datetime.utcnow() - h['timestamp']).days < 30]

        return {
            'records': recent_history,
            'avg_completion_time': statistics.mean(
                [h['completion_time'] for h in recent_history if h['completion_time']]) if recent_history else 0,
            'success_rate': sum(1 for h in recent_history if h['success']) / len(
                recent_history) if recent_history else 0
        }

    def _analyze_patterns(self, historical_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze historical patterns"""
        patterns = {}

        records = historical_data.get('records', [])
        if not records:
            return patterns

        # Find best performing time slots
        successful_records = [r for r in records if r['success']]
        if successful_records:
            # Group by hour
            hour_performance = defaultdict(list)
            for record in successful_records:
                hour = record['timestamp'].hour
                hour_performance[hour].append(record['completion_time'])

            # Find best hour
            best_hour = min(hour_performance.items(),
                            key=lambda x: statistics.mean(x[1]))[0]
            patterns['best_start_time'] = best_hour

        # Find optimal run sizes
        run_sizes = [r.get('actual_quantity', 0) for r in records if r.get('actual_quantity')]
        if run_sizes:
            patterns['optimal_run_size'] = int(statistics.median(run_sizes))

        # Find optimal intervals
        # This would require more detailed tracking of portion performance
        patterns['optimal_interval'] = 20  # Default for now

        return patterns

    def _adjust_to_best_time(self, scheduled_time: datetime, best_hour: int) -> datetime:
        """Adjust scheduled time to best performing hour"""
        # If already in good time, keep it
        if abs(scheduled_time.hour - best_hour) <= 2:
            return scheduled_time

        # Adjust to best hour
        adjusted = scheduled_time.replace(hour=best_hour, minute=0, second=0)

        # If in the past, move to next day
        if adjusted < datetime.utcnow():
            adjusted += timedelta(days=1)

        return adjusted

    def _optimize_run_size(self, portion: Dict[str, Any], optimal_size: int) -> Dict[str, Any]:
        """Optimize run size based on historical data"""
        total_quantity = portion['quantity_per_run'] * portion['runs']

        # Calculate new runs based on optimal size
        new_runs = max(1, total_quantity // optimal_size)
        new_quantity_per_run = total_quantity // new_runs

        # Ensure we don't lose quantity due to rounding
        if new_quantity_per_run * new_runs < total_quantity:
            new_quantity_per_run += 1

        portion['runs'] = new_runs
        portion['quantity_per_run'] = new_quantity_per_run

        return portion

    def _adjust_interval(self, current_interval: int, optimal_interval: int) -> int:
        """Adjust interval based on historical performance"""
        # Gradual adjustment (don't change too drastically)
        diff = optimal_interval - current_interval
        adjustment = diff * 0.3  # 30% adjustment

        return max(5, int(current_interval + adjustment))

    def _validate_total_quantity(self, original: List[Dict], optimized: List[Dict]):
        """Ensure total quantity is preserved after optimization"""
        original_total = sum(
            p.get('quantity_per_run', 0) * p.get('runs', 1)
            for p in original
        )

        optimized_total = sum(
            p.get('quantity_per_run', 0) * p.get('runs', 1)
            for p in optimized
        )

        if original_total != optimized_total:
            # Adjust last portion
            diff = original_total - optimized_total
            if optimized and diff != 0:
                last = optimized[-1]
                last['quantity_per_run'] += diff // last.get('runs', 1)

    def _get_service_score(self, service_id: int) -> float:
        """Get performance score for service"""
        return self.service_scores.get(service_id, 0.5)  # Default 0.5

    def _update_service_score(self, service_id: int, success: bool, completion_time: float):
        """Update service performance score"""
        current_score = self.service_scores.get(service_id, 0.5)

        # Calculate performance score (0-1)
        time_score = 1.0 / (1.0 + completion_time / 3600)  # Faster is better
        success_score = 1.0 if success else 0.0

        new_score = (time_score + success_score) / 2

        # Exponential moving average
        alpha = 0.1  # Learning rate
        self.service_scores[service_id] = current_score * (1 - alpha) + new_score * alpha

    def _get_channel_service_preference(self, channel_id: int, service_id: int) -> float:
        """Get channel-specific preference for service"""
        if channel_id in self.channel_preferences:
            return self.channel_preferences[channel_id].get(service_id, 0.5)
        return 0.5

    def _update_channel_preference(self, channel_id: int, service_id: int, success: bool):
        """Update channel preference for service"""
        if channel_id not in self.channel_preferences:
            self.channel_preferences[channel_id] = {}

        current = self.channel_preferences[channel_id].get(service_id, 0.5)
        adjustment = 0.05 if success else -0.05

        # Keep in bounds [0, 1]
        new_preference = max(0.0, min(1.0, current + adjustment))
        self.channel_preferences[channel_id][service_id] = new_preference

    def _calculate_quantity_fit_score(self, quantity: int, min_qty: int, max_qty: int) -> float:
        """Calculate how well quantity fits service limits"""
        if quantity < min_qty or quantity > max_qty:
            return 0.0

        # Best score when quantity is in the middle of limits
        range_size = max_qty - min_qty
        if range_size == 0:
            return 1.0

        position = (quantity - min_qty) / range_size

        # Prefer middle range (not too close to limits)
        if position < 0.1 or position > 0.9:
            return 0.7
        elif position < 0.2 or position > 0.8:
            return 0.85
        else:
            return 1.0

    def _distribute_quantity(self, total: int, portions: int) -> List[int]:
        """Distribute quantity with decreasing amounts"""
        if portions <= 0:
            return []

        if portions == 1:
            return [total]

        # Use geometric progression for natural distribution
        ratio = 0.8  # Each portion is 80% of previous

        # Calculate first portion size
        # Sum of geometric series: a * (1 - r^n) / (1 - r)
        first_portion = total * (1 - ratio) / (1 - ratio ** portions)

        quantities = []
        remaining = total

        for i in range(portions):
            if i == portions - 1:
                # Last portion gets all remaining
                qty = remaining
            else:
                qty = int(first_portion * (ratio ** i))
                remaining -= qty

            quantities.append(qty)

        return quantities

    def _calculate_avg_completion_time(self) -> float:
        """Calculate average completion time across all records"""
        all_times = []
        for records in self.performance_history.values():
            all_times.extend([r['completion_time'] for r in records if r.get('completion_time')])

        return statistics.mean(all_times) if all_times else 0.0

    def _calculate_overall_success_rate(self) -> float:
        """Calculate overall success rate"""
        total = 0
        successful = 0

        for records in self.performance_history.values():
            total += len(records)
            successful += sum(1 for r in records if r.get('success', False))

        return successful / total if total > 0 else 0.0