"""
Database models (dataclasses) - Updated for correct DB structure
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from decimal import Decimal
import json


@dataclass
class Channel:
    """Channel model"""
    id: int
    channel_username: str
    channel_id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Channel':
        """Create instance from database row"""
        return cls(**row)


@dataclass
class ChannelSettings:
    """Channel settings model with proper JSON handling"""
    id: int
    channel_id: int
    views_target: int
    reactions_target: int
    reposts_target: int
    reaction_types: Union[List[str], str] = field(default_factory=list)
    views_service_id: Optional[int] = None
    reactions_service_id: Optional[int] = None
    reposts_service_id: Optional[int] = None
    randomize_reactions: bool = True
    randomize_reposts: bool = True
    randomize_percent: int = 40
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def __post_init__(self):
        """Handle JSON fields after initialization"""
        # Parse reaction_types if it's a string (from DB)
        if isinstance(self.reaction_types, str):
            try:
                self.reaction_types = json.loads(self.reaction_types)
            except json.JSONDecodeError:
                self.reaction_types = []
        elif self.reaction_types is None:
            self.reaction_types = []

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'ChannelSettings':
        """Create instance from database row"""
        data = dict(row)
        return cls(**data)

    def get_reaction_services(self) -> List[Dict[str, Any]]:
        """Parse reaction_types into service configurations"""
        if not self.reaction_types:
            return []

        services = []
        for item in self.reaction_types:
            if isinstance(item, str):
                # Old format: just emoji
                services.append({
                    'service_id': self.reactions_service_id,
                    'emoji': item,
                    'quantity': self.reactions_target // len(self.reaction_types)
                })
            elif isinstance(item, dict):
                # New format: {"service_id": 123, "quantity": 45}
                services.append(item)

        return services


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

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Post':
        """Create instance from database row"""
        return cls(**row)

    def is_new(self) -> bool:
        """Check if post is new"""
        return self.status == 'new'

    def is_processing(self) -> bool:
        """Check if post is being processed"""
        return self.status == 'processing'

    def is_completed(self) -> bool:
        """Check if post processing is completed"""
        return self.status == 'completed'


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

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Order':
        """Create instance from database row"""
        return cls(**row)

    def is_active(self) -> bool:
        """Check if order is active"""
        return self.status in ('pending', 'in_progress')


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

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'OrderPortion':
        """Create instance from database row"""
        return cls(**row)

    def total_quantity(self) -> int:
        """Calculate total quantity for this portion"""
        return self.quantity_per_run * self.runs

    def is_ready(self) -> bool:
        """Check if portion is ready to execute"""
        if self.status != 'waiting':
            return False
        if not self.scheduled_at:
            return True
        return datetime.utcnow() >= self.scheduled_at


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

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Service':
        """Create instance from database row"""
        data = dict(row)
        # Ensure price_per_1000 is Decimal
        if 'price_per_1000' in data and not isinstance(data['price_per_1000'], Decimal):
            data['price_per_1000'] = Decimal(str(data['price_per_1000']))
        return cls(**data)

    def calculate_cost(self, quantity: int) -> Decimal:
        """Calculate cost for given quantity"""
        return (Decimal(quantity) / Decimal(1000)) * self.price_per_1000

    def validate_quantity(self, quantity: int) -> bool:
        """Check if quantity is within service limits"""
        return self.min_quantity <= quantity <= self.max_quantity


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

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'PortionTemplate':
        """Create instance from database row"""
        data = dict(row)
        # Ensure quantity_percent is Decimal
        if 'quantity_percent' in data and not isinstance(data['quantity_percent'], Decimal):
            data['quantity_percent'] = Decimal(str(data['quantity_percent']))
        return cls(**data)

    def calculate_quantity(self, total: int) -> int:
        """Calculate quantity for this portion"""
        return int(total * float(self.quantity_percent) / 100)


@dataclass
class ChannelReactionService:
    """Channel reaction service model"""
    id: int
    channel_id: int
    service_id: int
    emoji: str
    target_quantity: int
    created_at: datetime

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'ChannelReactionService':
        """Create instance from database row"""
        return cls(**row)


@dataclass
class ApiKey:
    """API key model"""
    id: int
    service_name: str
    api_key: str
    is_active: bool
    created_at: datetime

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'ApiKey':
        """Create instance from database row"""
        return cls(**row)


@dataclass
class Log:
    """Log entry model"""
    id: int
    level: str
    message: str
    context: Union[Dict[str, Any], str]
    created_at: datetime

    def __post_init__(self):
        """Handle JSON context field"""
        if isinstance(self.context, str):
            try:
                self.context = json.loads(self.context)
            except json.JSONDecodeError:
                self.context = {'raw': self.context}

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Log':
        """Create instance from database row"""
        return cls(**row)


# Type aliases for clarity
ChannelID = int
PostID = int
OrderID = int
ServiceID = int
NakrutkaServiceID = int


# Helper dataclasses for complex operations

@dataclass
class OrderRequest:
    """Request to create an order"""
    post_id: PostID
    service_type: str
    service_id: NakrutkaServiceID
    total_quantity: int
    portions: List[Dict[str, Any]]
    start_delay_minutes: int = 0


@dataclass
class ChannelStats:
    """Channel statistics"""
    channel_id: ChannelID
    channel_username: str
    total_posts: int
    completed_posts: int
    processing_posts: int
    failed_posts: int
    success_rate: float
    total_cost: Decimal

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'ChannelStats':
        """Create instance from database row"""
        data = dict(row)
        # Calculate success rate
        total = data.get('total_posts', 0)
        completed = data.get('completed_posts', 0)
        data['success_rate'] = (completed / total * 100) if total > 0 else 0.0
        return cls(**data)


@dataclass
class ServiceCache:
    """Cache for services to avoid frequent DB queries"""
    services: Dict[NakrutkaServiceID, Service] = field(default_factory=dict)
    last_updated: datetime = field(default_factory=datetime.utcnow)

    def add_service(self, service: Service):
        """Add service to cache"""
        self.services[service.nakrutka_id] = service

    def get_service(self, nakrutka_id: NakrutkaServiceID) -> Optional[Service]:
        """Get service from cache"""
        return self.services.get(nakrutka_id)

    def is_stale(self, max_age_minutes: int = 60) -> bool:
        """Check if cache is stale"""
        age = datetime.utcnow() - self.last_updated
        return age.total_seconds() > max_age_minutes * 60

    def clear(self):
        """Clear cache"""
        self.services.clear()
        self.last_updated = datetime.utcnow()