"""
Database models (dataclasses)
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any
from decimal import Decimal


@dataclass
class Channel:
    """Channel model"""
    id: int
    channel_username: str
    channel_id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime


@dataclass
class ChannelSettings:
    """Channel settings model"""
    id: int
    channel_id: int
    views_target: int
    reactions_target: int
    reposts_target: int
    reaction_types: List[str]
    views_service_id: Optional[int]
    reactions_service_id: Optional[int]
    reposts_service_id: Optional[int]
    randomize_reactions: bool
    randomize_reposts: bool
    randomize_percent: int
    updated_at: datetime


@dataclass
class Post:
    """Post model"""
    id: int
    channel_id: int
    post_id: int
    post_url: Optional[str]
    published_at: datetime
    processed_at: Optional[datetime]
    status: str
    created_at: datetime


@dataclass
class Order:
    """Order model"""
    id: int
    post_id: int
    nakrutka_order_id: Optional[str]
    service_type: str
    service_id: int
    total_quantity: int
    status: str
    start_delay_minutes: int
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]


@dataclass
class OrderPortion:
    """Order portion model"""
    id: int
    order_id: int
    portion_number: int
    quantity_per_run: int
    runs: int
    interval_minutes: int
    nakrutka_portion_id: Optional[str]
    status: str
    scheduled_at: Optional[datetime]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]


@dataclass
class Service:
    """Service model"""
    id: int
    nakrutka_id: int
    service_name: str
    service_type: str
    price_per_1000: Decimal
    min_quantity: int
    max_quantity: int
    is_active: bool
    updated_at: datetime


@dataclass
class PortionTemplate:
    """Portion template model"""
    id: int
    channel_id: int
    service_type: str
    portion_number: int
    quantity_percent: Decimal
    runs_formula: str
    interval_minutes: int
    start_delay_minutes: int
    created_at: datetime


@dataclass
class ChannelReactionService:
    """Channel reaction service model"""
    id: int
    channel_id: int
    service_id: int
    emoji: str
    target_quantity: int
    created_at: datetime


@dataclass
class ApiKey:
    """API key model"""
    id: int
    service_name: str
    api_key: str
    is_active: bool
    created_at: datetime


@dataclass
class Log:
    """Log entry model"""
    id: int
    level: str
    message: str
    context: Dict[str, Any]
    created_at: datetime


# Type aliases for clarity
ChannelID = int
PostID = int
OrderID = int
ServiceID = int