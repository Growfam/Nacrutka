"""
Data validators for the bot
"""
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timedelta
import re

from src.database.models import *
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ValidationError(Exception):
    """Custom validation error"""
    pass


class DataValidator:
    """Validates data before processing"""

    @staticmethod
    def validate_channel(channel: Channel) -> Tuple[bool, Optional[str]]:
        """Validate channel data"""
        if not channel.channel_username:
            return False, "Channel username is empty"

        if not channel.channel_id:
            return False, "Channel ID is empty"

        # Validate username format
        username = channel.channel_username.lstrip('@')
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]{4,31}$', username):
            return False, f"Invalid channel username format: {username}"

        # Validate channel ID format
        if not str(channel.channel_id).startswith('-100'):
            return False, f"Invalid channel ID format: {channel.channel_id}"

        return True, None

    @staticmethod
    def validate_channel_settings(settings: ChannelSettings) -> Tuple[bool, Optional[str]]:
        """Validate channel settings"""
        # Check targets
        if settings.views_target < 0:
            return False, "Views target cannot be negative"

        if settings.reactions_target < 0:
            return False, "Reactions target cannot be negative"

        if settings.reposts_target < 0:
            return False, "Reposts target cannot be negative"

        # Check if at least one target is set
        if (settings.views_target == 0 and
                settings.reactions_target == 0 and
                settings.reposts_target == 0):
            return False, "At least one target must be greater than 0"

        # Check randomization
        if settings.randomize_percent < 0 or settings.randomize_percent > 100:
            return False, f"Randomize percent must be 0-100, got {settings.randomize_percent}"

        # Check service IDs
        if settings.views_target > 0 and not settings.views_service_id:
            return False, "Views service ID not set"

        if settings.reactions_target > 0 and not settings.reactions_service_id:
            return False, "Reactions service ID not set"

        if settings.reposts_target > 0 and not settings.reposts_service_id:
            return False, "Reposts service ID not set"

        return True, None

    @staticmethod
    def validate_post(post: Post) -> Tuple[bool, Optional[str]]:
        """Validate post data"""
        if not post.channel_id:
            return False, "Channel ID is empty"

        if not post.post_id:
            return False, "Post ID is empty"

        if post.post_id <= 0:
            return False, f"Invalid post ID: {post.post_id}"

        # Check URL format if provided
        if post.post_url:
            url_pattern = r'https?://t\.me/[a-zA-Z0-9_]+/\d+'
            if not re.match(url_pattern, post.post_url):
                return False, f"Invalid post URL format: {post.post_url}"

        # Check post age
        if post.published_at:
            age = datetime.utcnow() - post.published_at
            if age.total_seconds() < 0:
                return False, "Post from future"
            if age.days > 7:
                return False, "Post too old (>7 days)"

        return True, None

    @staticmethod
    def validate_order_quantity(
            quantity: int,
            service: Service
    ) -> Tuple[bool, Optional[str]]:
        """Validate order quantity against service limits"""
        if quantity <= 0:
            return False, "Quantity must be positive"

        if quantity < service.min_quantity:
            return False, f"Quantity {quantity} below minimum {service.min_quantity}"

        if quantity > service.max_quantity:
            return False, f"Quantity {quantity} above maximum {service.max_quantity}"

        return True, None

    @staticmethod
    def validate_portions(
            portions: List[Dict[str, Any]],
            total_target: int
    ) -> Tuple[bool, Optional[str]]:
        """Validate that portions are correct"""
        if not portions:
            return False, "No portions provided"

        if len(portions) == 0:
            return False, "Empty portions list"

        # Calculate total from portions
        total_calculated = sum(
            p.get('quantity_per_run', 0) * p.get('runs', 1)
            for p in portions
        )

        # Allow 5% deviation
        deviation = abs(total_calculated - total_target) / total_target if total_target > 0 else 0

        if deviation > 0.05:
            return False, f"Portions sum {total_calculated} deviates from target {total_target} by {deviation * 100:.1f}%"

        # Check each portion
        for i, portion in enumerate(portions):
            if portion.get('quantity_per_run', 0) <= 0:
                return False, f"Portion {i + 1} has invalid quantity_per_run"

            if portion.get('runs', 0) <= 0:
                return False, f"Portion {i + 1} has invalid runs"

            if portion.get('interval_minutes', 0) < 0:
                return False, f"Portion {i + 1} has negative interval"

        return True, None

    @staticmethod
    def validate_reaction_distribution(
            distribution: List[Dict[str, Any]],
            total_target: int
    ) -> Tuple[bool, Optional[str]]:
        """Validate reaction distribution"""
        if not distribution:
            return False, "No reaction distribution provided"

        # Check total
        total = sum(item.get('target_quantity', 0) for item in distribution)

        # Should match target (allow some deviation for rounding)
        if abs(total - total_target) > len(distribution):
            return False, f"Distribution sum {total} doesn't match target {total_target}"

        # Check each item
        for item in distribution:
            if not item.get('service_id'):
                return False, "Reaction distribution missing service_id"

            if item.get('target_quantity', 0) <= 0:
                return False, f"Invalid quantity for service {item.get('service_id')}"

        return True, None

    @staticmethod
    def validate_api_response(response: Any, expected_fields: List[str]) -> Tuple[bool, Optional[str]]:
        """Validate API response structure"""
        if not response:
            return False, "Empty response"

        if not isinstance(response, dict):
            return False, f"Response is not a dict, got {type(response)}"

        # Check for error
        if 'error' in response:
            return False, f"API error: {response.get('error')}"

        # Check expected fields
        missing_fields = [field for field in expected_fields if field not in response]
        if missing_fields:
            return False, f"Missing fields in response: {missing_fields}"

        return True, None

    @staticmethod
    def validate_telegram_url(url: str) -> Tuple[bool, Optional[str]]:
        """Validate Telegram URL"""
        patterns = [
            r'^https://t\.me/[a-zA-Z0-9_]+/\d+$',
            r'^https://t\.me/c/\d+/\d+$',
            r'^http://t\.me/[a-zA-Z0-9_]+/\d+$',
        ]

        for pattern in patterns:
            if re.match(pattern, url):
                return True, None

        return False, f"Invalid Telegram URL format: {url}"

    @staticmethod
    def validate_portion_templates(
            templates: List[PortionTemplate],
            service_type: str
    ) -> Tuple[bool, Optional[str]]:
        """Validate portion templates"""
        if not templates:
            return False, f"No templates for {service_type}"

        # Check portion numbers
        portion_numbers = [t.portion_number for t in templates]
        expected = list(range(1, len(templates) + 1))

        if sorted(portion_numbers) != expected:
            return False, f"Invalid portion numbers: {portion_numbers}"

        # Check total percentage
        total_percent = sum(float(t.quantity_percent) for t in templates)

        if abs(total_percent - 100.0) > 0.1:
            return False, f"Total percent {total_percent} != 100%"

        # Check formulas
        for template in templates:
            if not template.runs_formula:
                return False, f"Empty runs_formula for portion {template.portion_number}"

            # Validate formula format
            if not (template.runs_formula.startswith('quantity/') or
                    template.runs_formula.isdigit()):
                return False, f"Invalid runs_formula: {template.runs_formula}"

        return True, None


class OrderValidator:
    """Validates orders before sending to Nakrutka"""

    def __init__(self, service_cache: Dict[int, Service]):
        self.service_cache = service_cache

    def validate_order_params(
            self,
            service_id: int,
            quantity: int,
            link: str
    ) -> Tuple[bool, Optional[str]]:
        """Validate order parameters"""
        # Check service exists
        if service_id not in self.service_cache:
            return False, f"Service {service_id} not found"

        service = self.service_cache[service_id]

        # Validate quantity
        valid, error = DataValidator.validate_order_quantity(quantity, service)
        if not valid:
            return False, error

        # Validate URL
        valid, error = DataValidator.validate_telegram_url(link)
        if not valid:
            return False, error

        return True, None

    def validate_drip_feed_params(
            self,
            service_id: int,
            portions: List[Dict[str, Any]]
    ) -> Tuple[bool, Optional[str]]:
        """Validate drip-feed parameters"""
        if not portions:
            return False, "No portions provided"

        service = self.service_cache.get(service_id)
        if not service:
            return False, f"Service {service_id} not found"

        # Check each portion
        for i, portion in enumerate(portions):
            quantity = portion.get('quantity_per_run', 0)
            runs = portion.get('runs', 0)
            interval = portion.get('interval_minutes', 0)

            # Check quantity
            if quantity < service.min_quantity:
                return False, f"Portion {i + 1} quantity {quantity} below minimum"

            # Check total for portion
            total = quantity * runs
            if total > service.max_quantity:
                return False, f"Portion {i + 1} total {total} above maximum"

            # Check interval
            if interval < 0:
                return False, f"Portion {i + 1} has negative interval"

            if runs > 1 and interval < 5:
                return False, f"Portion {i + 1} interval too small for drip-feed"

        return True, None


class ConfigValidator:
    """Validates configuration and settings"""

    @staticmethod
    def validate_environment() -> List[str]:
        """Validate environment variables"""
        from src.config import settings

        errors = []

        # Required variables
        if not settings.database_url:
            errors.append("DATABASE_URL not set")

        if not settings.nakrutka_api_key:
            errors.append("NAKRUTKA_API_KEY not set")

        if not settings.telegram_bot_token:
            errors.append("TELEGRAM_BOT_TOKEN not set")

        # Validate formats
        if settings.database_url and not settings.database_url.startswith('postgresql://'):
            errors.append("DATABASE_URL must start with postgresql://")

        if settings.telegram_bot_token and ':' not in settings.telegram_bot_token:
            errors.append("Invalid TELEGRAM_BOT_TOKEN format")

        # Check intervals
        if settings.check_interval < 10:
            errors.append("CHECK_INTERVAL too small (<10 seconds)")

        if settings.retry_delay < 1:
            errors.append("RETRY_DELAY too small (<1 second)")

        return errors

    @staticmethod
    def validate_database_schema(integrity_result: Dict[str, Any]) -> List[str]:
        """Validate database schema integrity"""
        errors = []

        # Check required tables
        required_tables = [
            'channels', 'channel_settings', 'posts', 'orders',
            'order_portions', 'services', 'portion_templates',
            'channel_reaction_services', 'api_keys', 'logs'
        ]

        existing_tables = list(integrity_result.get('tables', {}).keys())
        missing_tables = [t for t in required_tables if t not in existing_tables]

        if missing_tables:
            errors.append(f"Missing tables: {missing_tables}")

        # Check required functions
        required_functions = [
            'calculate_random_quantity',
            'calculate_portion_details',
            'update_updated_at_column'
        ]

        existing_functions = list(integrity_result.get('functions', {}).keys())
        missing_functions = [f for f in required_functions if f not in existing_functions]

        if missing_functions:
            errors.append(f"Missing functions: {missing_functions}")

        # Check views
        required_views = ['channel_monitoring']
        existing_views = list(integrity_result.get('views', {}).keys())
        missing_views = [v for v in required_views if v not in existing_views]

        if missing_views:
            errors.append(f"Missing views: {missing_views}")

        return errors


def validate_before_processing(
        post: Post,
        channel_settings: ChannelSettings,
        portion_templates: Dict[str, List[PortionTemplate]]
) -> List[str]:
    """Comprehensive validation before processing a post"""
    errors = []

    # Validate post
    valid, error = DataValidator.validate_post(post)
    if not valid:
        errors.append(f"Post validation: {error}")

    # Validate settings
    valid, error = DataValidator.validate_channel_settings(channel_settings)
    if not valid:
        errors.append(f"Settings validation: {error}")

    # Validate templates for each service type
    for service_type in ['views', 'reactions', 'reposts']:
        templates = portion_templates.get(service_type, [])

        # Skip if no target for this type
        if service_type == 'views' and channel_settings.views_target == 0:
            continue
        if service_type == 'reactions' and channel_settings.reactions_target == 0:
            continue
        if service_type == 'reposts' and channel_settings.reposts_target == 0:
            continue

        valid, error = DataValidator.validate_portion_templates(templates, service_type)
        if not valid:
            errors.append(f"Templates validation for {service_type}: {error}")

    return errors