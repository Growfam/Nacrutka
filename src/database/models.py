"""
Database models and enums
"""
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, List, Any
from datetime import datetime


class PostStatus(str, Enum):
    """Post processing status"""
    NEW = "new"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class OrderStatus(str, Enum):
    """Order execution status"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    AWAITING = "awaiting"
    CANCELED = "canceled"
    FAILED = "failed"
    PARTIAL = "partial"


class ServiceType(str, Enum):
    """SMM service types"""
    VIEWS = "views"
    REACTIONS = "reactions"
    REPOSTS = "reposts"


@dataclass
class Channel:
    """Telegram channel model"""
    id: int  # Telegram channel ID
    username: Optional[str]
    title: str
    is_active: bool = True
    monitoring_interval: int = 30  # seconds
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "username": self.username,
            "title": self.title,
            "is_active": self.is_active,
            "monitoring_interval": self.monitoring_interval,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }


@dataclass
class ChannelSettings:
    """Channel SMM settings model"""
    id: Optional[int]
    channel_id: int
    service_type: ServiceType

    # Base settings
    base_quantity: int
    randomization_percent: int = 0

    # Portion settings
    portions_count: int = 5
    fast_delivery_percent: int = 70

    # Reaction specific
    reaction_distribution: Optional[Dict[str, int]] = None  # {"👍": 45, "❤️": 30}

    # Repost specific
    repost_delay_minutes: int = 5

    # Twiboost service mappings
    twiboost_service_ids: Optional[Dict[str, int]] = None  # {"service_name": service_id}

    # Drip-feed settings
    drops_per_run: int = 5  # количество за один запуск
    run_interval: int = 30  # интервал между запусками в минутах

    updated_at: Optional[datetime] = None

    def calculate_runs(self, total_quantity: int) -> int:
        """Calculate number of runs for drip-feed"""
        if self.drops_per_run > 0:
            return max(1, total_quantity // self.drops_per_run)
        return 1

    def get_reaction_quantities(self, total: int) -> Dict[str, int]:
        """Calculate quantities for each reaction type"""
        if not self.reaction_distribution:
            return {}

        result = {}
        remaining = total

        # Sort by percentage to handle rounding
        sorted_reactions = sorted(
            self.reaction_distribution.items(),
            key=lambda x: x[1],
            reverse=True
        )

        for emoji, percentage in sorted_reactions[:-1]:
            quantity = int(total * percentage / 100)
            result[emoji] = quantity
            remaining -= quantity

        # Give remaining to last reaction
        if sorted_reactions:
            last_emoji = sorted_reactions[-1][0]
            result[last_emoji] = remaining

        return result


@dataclass
class Post:
    """Telegram post model"""
    id: Optional[int]
    channel_id: int
    message_id: int
    content: Optional[str]
    status: PostStatus = PostStatus.NEW
    detected_at: datetime = None
    processed_at: Optional[datetime] = None
    channel_username: Optional[str] = None  # ДОДАНО: username каналу

    @property
    def link(self) -> str:
        """Generate Telegram link to post"""
        # ВИПРАВЛЕНО: Використовуємо правильний формат посилань

        # Якщо є username - використовуємо публічне посилання
        if self.channel_username:
            return f"https://t.me/{self.channel_username}/{self.message_id}"

        # Для приватних каналів використовуємо формат /c/
        # Видаляємо -100 префікс для приватних каналів
        channel_str = str(abs(self.channel_id))
        if len(channel_str) > 10:  # Private channel має префікс -100
            channel_str = channel_str[3:]  # Видаляємо перші 3 символи (100)

        return f"https://t.me/c/{channel_str}/{self.message_id}"


@dataclass
class Order:
    """SMM order model"""
    id: Optional[int]
    post_id: int
    twiboost_order_id: Optional[int]
    service_type: ServiceType
    service_id: int  # Twiboost service ID

    # Quantities
    quantity: int  # Original requested
    actual_quantity: int  # After randomization

    # Portion info
    portion_number: int = 1
    portion_size: int = 0

    # Drip-feed settings
    runs: Optional[int] = None
    interval: Optional[int] = None  # minutes

    status: OrderStatus = OrderStatus.PENDING

    # Timestamps
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Response data
    response_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

    # For reactions
    reaction_emoji: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "post_id": self.post_id,
            "twiboost_order_id": self.twiboost_order_id,
            "service_type": self.service_type,
            "service_id": self.service_id,
            "quantity": self.quantity,
            "actual_quantity": self.actual_quantity,
            "portion_number": self.portion_number,
            "portion_size": self.portion_size,
            "runs": self.runs,
            "interval": self.interval,
            "status": self.status,
            "reaction_emoji": self.reaction_emoji,
            "scheduled_at": self.scheduled_at.isoformat() if self.scheduled_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message
        }


@dataclass
class ExecutionLog:
    """Execution log model"""
    id: Optional[int]
    order_id: int
    action: str
    details: Dict[str, Any]
    created_at: datetime = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "order_id": self.order_id,
            "action": self.action,
            "details": self.details,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }


@dataclass
class TwiboostService:
    """Twiboost service mapping"""
    service_id: int
    name: str
    type: str
    category: str
    rate: float
    min_quantity: int
    max_quantity: int
    refill: bool
    cancel: bool

    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> "TwiboostService":
        """Create from API response"""
        return cls(
            service_id=data["service"],
            name=data["name"],
            type=data["type"],
            category=data.get("category", ""),
            rate=float(data["rate"]),
            min_quantity=data["min"],
            max_quantity=data["max"],
            refill=data.get("refill", False),
            cancel=data.get("cancel", False)
        )