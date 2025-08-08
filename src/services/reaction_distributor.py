"""
Advanced reaction distribution system for multi-service reactions
"""
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
import random
import math

from src.database.models import (
    ChannelReactionService,
    Service,
    ChannelSettings,
    PortionTemplate
)
from src.database.queries import Queries
from src.utils.logger import get_logger
from src.utils.cache_manager import CacheManager

logger = get_logger(__name__)


@dataclass
class ReactionAllocation:
    """Single reaction allocation"""
    service_id: int
    emoji: str
    base_quantity: int
    randomized_quantity: int
    proportion: float
    service_info: Optional[Service] = None


@dataclass
class ReactionPlan:
    """Complete reaction distribution plan"""
    total_quantity: int
    allocations: List[ReactionAllocation]
    uses_randomization: bool
    randomization_percent: int

    def get_total_allocated(self) -> int:
        """Get total allocated reactions"""
        return sum(a.randomized_quantity for a in self.allocations)

    def validate(self) -> Tuple[bool, Optional[str]]:
        """Validate the plan"""
        total = self.get_total_allocated()

        # Allow 1% deviation for rounding
        deviation = abs(total - self.total_quantity) / self.total_quantity
        if deviation > 0.01:
            return False, f"Total allocated {total} deviates from target {self.total_quantity}"

        # Check each allocation
        for alloc in self.allocations:
            if alloc.randomized_quantity <= 0:
                return False, f"Invalid quantity for {alloc.emoji}"

            if alloc.service_info:
                if alloc.randomized_quantity < alloc.service_info.min_quantity:
                    return False, f"{alloc.emoji} quantity below minimum"
                if alloc.randomized_quantity > alloc.service_info.max_quantity:
                    return False, f"{alloc.emoji} quantity above maximum"

        return True, None


class ReactionDistributor:
    """Advanced reaction distribution with multi-service support"""

    def __init__(self, queries: Queries, cache_manager: CacheManager):
        self.queries = queries
        self.cache = cache_manager
        self._service_cache = {}

    async def create_reaction_plan(
            self,
            channel_settings: ChannelSettings,
            reaction_services: List[ChannelReactionService],
            total_quantity: int,
            use_randomization: bool = True
    ) -> ReactionPlan:
        """
        Create optimized reaction distribution plan

        Args:
            channel_settings: Channel settings with randomization config
            reaction_services: List of reaction services for channel
            total_quantity: Total reactions to distribute (after randomization)
            use_randomization: Whether to apply additional micro-randomization

        Returns:
            ReactionPlan with allocations
        """
        if not reaction_services:
            raise ValueError("No reaction services configured")

        # Load service information
        await self._load_services(reaction_services)

        # Calculate base proportions
        total_base = sum(rs.target_quantity for rs in reaction_services)
        if total_base <= 0:
            raise ValueError("Total reaction targets is 0")

        allocations = []
        remaining = total_quantity

        # Sort by target quantity descending to allocate larger amounts first
        sorted_services = sorted(
            reaction_services,
            key=lambda x: x.target_quantity,
            reverse=True
        )

        for i, rs in enumerate(sorted_services):
            # Calculate base proportion
            proportion = rs.target_quantity / total_base

            # Calculate quantity for this service
            if i == len(sorted_services) - 1:
                # Last service gets all remaining
                quantity = remaining
            else:
                quantity = int(total_quantity * proportion)

                # Apply micro-randomization if enabled
                if use_randomization and channel_settings.randomize_reactions:
                    # Small randomization (±10%) for variety
                    variance = int(quantity * 0.1)
                    if variance > 0:
                        quantity += random.randint(-variance, variance)

            # Ensure within service limits
            service = self._service_cache.get(rs.service_id)
            if service:
                quantity = self._adjust_for_limits(quantity, service)

            # Create allocation
            allocation = ReactionAllocation(
                service_id=rs.service_id,
                emoji=rs.emoji,
                base_quantity=int(total_quantity * proportion),
                randomized_quantity=quantity,
                proportion=proportion,
                service_info=service
            )

            allocations.append(allocation)
            remaining -= quantity

        # Handle any remaining due to rounding
        if remaining != 0 and allocations:
            # Add to largest allocation
            allocations[0].randomized_quantity += remaining

        # Create plan
        plan = ReactionPlan(
            total_quantity=total_quantity,
            allocations=allocations,
            uses_randomization=channel_settings.randomize_reactions,
            randomization_percent=channel_settings.randomize_percent
        )

        # Validate
        valid, error = plan.validate()
        if not valid:
            logger.error(f"Invalid reaction plan: {error}")
            # Try to fix by redistributing
            plan = await self._fix_reaction_plan(plan)

        return plan

    async def distribute_reactions_legacy(
            self,
            reaction_types: List[str],
            total_quantity: int,
            default_service_id: int
    ) -> ReactionPlan:
        """
        Legacy distribution for old format (just emoji list)
        Used when channel has reaction_types instead of reaction_services
        """
        if not reaction_types:
            # Single service with all reactions
            allocation = ReactionAllocation(
                service_id=default_service_id,
                emoji='mix',
                base_quantity=total_quantity,
                randomized_quantity=total_quantity,
                proportion=1.0
            )

            return ReactionPlan(
                total_quantity=total_quantity,
                allocations=[allocation],
                uses_randomization=False,
                randomization_percent=0
            )

        # Try to find services for each emoji
        allocations = []
        quantity_per_emoji = total_quantity // len(reaction_types)
        remaining = total_quantity

        for i, emoji in enumerate(reaction_types):
            # Try to find specific service for this emoji
            service = await self._find_service_for_emoji(emoji)
            service_id = service.nakrutka_id if service else default_service_id

            # Last emoji gets remaining
            if i == len(reaction_types) - 1:
                quantity = remaining
            else:
                quantity = quantity_per_emoji

            allocation = ReactionAllocation(
                service_id=service_id,
                emoji=emoji,
                base_quantity=quantity_per_emoji,
                randomized_quantity=quantity,
                proportion=1.0 / len(reaction_types),
                service_info=service
            )

            allocations.append(allocation)
            remaining -= quantity

        return ReactionPlan(
            total_quantity=total_quantity,
            allocations=allocations,
            uses_randomization=False,
            randomization_percent=0
        )

    async def optimize_reaction_distribution(
            self,
            plan: ReactionPlan,
            performance_data: Optional[Dict[int, float]] = None
    ) -> ReactionPlan:
        """
        Optimize distribution based on performance metrics

        Args:
            plan: Original plan
            performance_data: Dict of service_id -> success_rate

        Returns:
            Optimized plan
        """
        if not performance_data or len(plan.allocations) < 2:
            return plan

        # Calculate weighted scores
        scores = {}
        for alloc in plan.allocations:
            success_rate = performance_data.get(alloc.service_id, 0.8)

            # Score based on success rate and cost
            if alloc.service_info:
                # Lower price is better
                price_score = 1.0 / (1.0 + float(alloc.service_info.price_per_1000))
            else:
                price_score = 0.5

            # Combined score (70% success, 30% price)
            scores[alloc.service_id] = (0.7 * success_rate) + (0.3 * price_score)

        # Redistribute based on scores
        total_score = sum(scores.values())
        if total_score <= 0:
            return plan

        # Create new allocations
        new_allocations = []
        remaining = plan.total_quantity

        # Sort by score descending
        sorted_allocs = sorted(
            plan.allocations,
            key=lambda a: scores.get(a.service_id, 0),
            reverse=True
        )

        for i, alloc in enumerate(sorted_allocs):
            score = scores.get(alloc.service_id, 0)
            weight = score / total_score

            if i == len(sorted_allocs) - 1:
                quantity = remaining
            else:
                quantity = int(plan.total_quantity * weight)

            # Ensure minimum quantity
            if quantity < 10 and plan.total_quantity > 100:
                quantity = 10

            # Adjust for limits
            if alloc.service_info:
                quantity = self._adjust_for_limits(quantity, alloc.service_info)

            new_alloc = ReactionAllocation(
                service_id=alloc.service_id,
                emoji=alloc.emoji,
                base_quantity=alloc.base_quantity,
                randomized_quantity=quantity,
                proportion=weight,
                service_info=alloc.service_info
            )

            new_allocations.append(new_alloc)
            remaining -= quantity

        return ReactionPlan(
            total_quantity=plan.total_quantity,
            allocations=new_allocations,
            uses_randomization=plan.uses_randomization,
            randomization_percent=plan.randomization_percent
        )

    async def calculate_reaction_portions(
            self,
            allocation: ReactionAllocation,
            templates: List[PortionTemplate]
    ) -> List[Dict[str, Any]]:
        """
        Calculate portions for single reaction allocation

        Returns:
            List of portion configurations
        """
        if not templates:
            # Single portion
            return [{
                'quantity_per_run': allocation.randomized_quantity,
                'runs': 1,
                'interval_minutes': 0,
                'start_delay_minutes': 0
            }]

        portions = []
        remaining = allocation.randomized_quantity

        for i, template in enumerate(templates):
            # Calculate quantity for this portion
            if i == len(templates) - 1:
                portion_quantity = remaining
            else:
                portion_quantity = int(
                    allocation.randomized_quantity * float(template.quantity_percent) / 100
                )

            if portion_quantity <= 0:
                continue

            # Parse runs formula
            if template.runs_formula.startswith('quantity/'):
                divisor = int(template.runs_formula.replace('quantity/', ''))
                quantity_per_run = divisor
                runs = math.ceil(portion_quantity / divisor)
            else:
                runs = int(template.runs_formula) if template.runs_formula.isdigit() else 1
                quantity_per_run = math.ceil(portion_quantity / runs)

            # Adjust for service limits
            if allocation.service_info:
                min_qty = allocation.service_info.min_quantity
                max_qty = allocation.service_info.max_quantity

                if quantity_per_run < min_qty:
                    quantity_per_run = min_qty
                    runs = math.ceil(portion_quantity / quantity_per_run)
                elif quantity_per_run > max_qty:
                    quantity_per_run = max_qty
                    runs = math.ceil(portion_quantity / quantity_per_run)

            portions.append({
                'portion_number': template.portion_number,
                'quantity_per_run': quantity_per_run,
                'runs': runs,
                'interval_minutes': template.interval_minutes,
                'start_delay_minutes': template.start_delay_minutes,
                'total_quantity': quantity_per_run * runs
            })

            remaining -= quantity_per_run * runs

        return portions

    async def _load_services(self, reaction_services: List[ChannelReactionService]):
        """Load service information for all reaction services"""
        for rs in reaction_services:
            if rs.service_id not in self._service_cache:
                # Try cache first
                cache_key = f"service:{rs.service_id}"
                service_data = await self.cache.get(cache_key)

                if service_data:
                    self._service_cache[rs.service_id] = Service(**service_data)
                else:
                    # Load from DB
                    service = await self.queries.get_service(rs.service_id)
                    if service:
                        self._service_cache[rs.service_id] = service
                        # Cache for 1 hour
                        await self.cache.set(
                            cache_key,
                            service.__dict__,
                            ttl=3600
                        )

    async def _find_service_for_emoji(self, emoji: str) -> Optional[Service]:
        """Find best service for given emoji"""
        # Check cache
        cache_key = f"emoji_service:{emoji}"
        service_id = await self.cache.get(cache_key)

        if service_id:
            return await self.queries.get_service(service_id)

        # Search by emoji
        service = await self.queries.find_service_by_emoji(emoji)
        if service:
            # Cache mapping
            await self.cache.set(cache_key, service.nakrutka_id, ttl=3600)
            return service

        # Try to find by name pattern
        emoji_names = {
            '❤️': 'сердце|heart',
            '🔥': 'огонь|fire',
            '👍': 'лайк|like|thumb',
            '😢': 'слеза|tear|cry',
            '💩': 'какашка|poop',
            '👎': 'дизлайк|dislike|thumb down'
        }

        pattern = emoji_names.get(emoji)
        if pattern:
            service = await self.queries.get_service_by_name_pattern(pattern)
            if service:
                await self.cache.set(cache_key, service.nakrutka_id, ttl=3600)
                return service

        return None

    def _adjust_for_limits(self, quantity: int, service: Service) -> int:
        """Adjust quantity to fit within service limits"""
        if quantity < service.min_quantity:
            return service.min_quantity
        elif quantity > service.max_quantity:
            return service.max_quantity
        return quantity

    async def _fix_reaction_plan(self, plan: ReactionPlan) -> ReactionPlan:
        """Try to fix invalid reaction plan"""
        total_allocated = plan.get_total_allocated()
        difference = plan.total_quantity - total_allocated

        if difference == 0:
            return plan

        # Find allocation that can handle the difference
        for alloc in plan.allocations:
            if alloc.service_info:
                new_quantity = alloc.randomized_quantity + difference

                # Check if within limits
                if (alloc.service_info.min_quantity <= new_quantity <=
                        alloc.service_info.max_quantity):
                    alloc.randomized_quantity = new_quantity
                    return plan

        # If can't fix within limits, distribute proportionally
        if difference > 0:
            # Need to add more
            for alloc in plan.allocations:
                if alloc.service_info and alloc.randomized_quantity < alloc.service_info.max_quantity:
                    can_add = alloc.service_info.max_quantity - alloc.randomized_quantity
                    to_add = min(can_add, difference)
                    alloc.randomized_quantity += to_add
                    difference -= to_add

                    if difference == 0:
                        break
        else:
            # Need to remove
            difference = abs(difference)
            for alloc in reversed(plan.allocations):
                if alloc.service_info and alloc.randomized_quantity > alloc.service_info.min_quantity:
                    can_remove = alloc.randomized_quantity - alloc.service_info.min_quantity
                    to_remove = min(can_remove, difference)
                    alloc.randomized_quantity -= to_remove
                    difference -= to_remove

                    if difference == 0:
                        break

        return plan

    def get_emoji_groups(self) -> Dict[str, List[str]]:
        """Get emoji groups for better distribution"""
        return {
            'positive': ['❤️', '🔥', '👍', '🎉', '🥰', '😍', '💯', '🏆'],
            'negative': ['👎', '😢', '💩', '🤮', '😱', '🤬', '😡'],
            'neutral': ['🤔', '😐', '🤷', '👀'],
            'premium': ['🚀', '💔', '😈', '🌚', '👾', '🐳'],
            'seasonal': ['🎄', '🎅', '☃️', '🎃', '👻']
        }

    def suggest_reaction_mix(
            self,
            channel_type: str,
            total_reactions: int
    ) -> Dict[str, int]:
        """
        Suggest reaction distribution based on channel type

        Args:
            channel_type: Type of content (news, memes, crypto, etc)
            total_reactions: Total reactions to distribute

        Returns:
            Dict of emoji -> suggested quantity
        """
        suggestions = {
            'news': {
                '👍': 0.30,
                '❤️': 0.25,
                '🔥': 0.20,
                '🤔': 0.15,
                '😢': 0.10
            },
            'memes': {
                '🤣': 0.35,
                '🔥': 0.25,
                '❤️': 0.20,
                '😂': 0.10,
                '💩': 0.10
            },
            'crypto': {
                '🚀': 0.30,
                '💰': 0.25,
                '🔥': 0.20,
                '📈': 0.15,
                '💎': 0.10
            },
            'default': {
                '❤️': 0.35,
                '🔥': 0.25,
                '👍': 0.20,
                '🎉': 0.10,
                '😍': 0.10
            }
        }

        distribution = suggestions.get(channel_type, suggestions['default'])

        result = {}
        remaining = total_reactions

        items = list(distribution.items())
        for i, (emoji, proportion) in enumerate(items):
            if i == len(items) - 1:
                # Last emoji gets remaining
                quantity = remaining
            else:
                quantity = int(total_reactions * proportion)

            result[emoji] = quantity
            remaining -= quantity

        return result