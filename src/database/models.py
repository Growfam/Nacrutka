"""
Database models (dataclasses) - Enhanced for universal channel support
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any, Union, Tuple
from decimal import Decimal
import json
from enum import Enum


class ServiceType(str, Enum):
    """Service type enum"""
    VIEWS = "views"
    REACTIONS = "reactions"
    REPOSTS = "reposts"
    SUBSCRIBERS = "subscribers"


class OrderStatus(str, Enum):
    """Order status enum"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


class PostStatus(str, Enum):
    """Post status enum"""
    NEW = "new"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class PortionStatus(str, Enum):
    """Portion status enum"""
    WAITING = "waiting"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


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
        return cls(**{k: v for k, v in row.items() if k in cls.__annotations__})

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'id': self.id,
            'channel_username': self.channel_username,
            'channel_id': self.channel_id,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


@dataclass
class ChannelSettings:
    """Enhanced channel settings model"""
    id: int
    channel_id: int
    views_target: int
    reactions_target: int
    reposts_target: int
    reaction_types: List[str] = field(default_factory=list)  # Legacy field
    views_service_id: Optional[int] = None
    reactions_service_id: Optional[int] = None  # Default service ID
    reposts_service_id: Optional[int] = None
    randomize_reactions: bool = True
    randomize_reposts: bool = True
    randomize_percent: int = 40
    process_old_posts_count: int = 0
    max_post_age_hours: int = 24
    updated_at: datetime = field(default_factory=datetime.utcnow)

    # Additional fields for enhanced functionality
    _reaction_distribution: Optional[List[Dict[str, Any]]] = field(default=None, init=False)
    _service_overrides: Optional[Dict[str, int]] = field(default=None, init=False)

    def __post_init__(self):
        """Handle JSON fields and legacy compatibility"""
        # Parse reaction_types if it's a JSON string
        if isinstance(self.reaction_types, str):
            try:
                parsed = json.loads(self.reaction_types)
                # Handle different formats
                if isinstance(parsed, list):
                    if all(isinstance(item, str) for item in parsed):
                        # Simple emoji list - keep as is
                        self.reaction_types = parsed
                    elif all(isinstance(item, dict) for item in parsed):
                        # New format with service IDs
                        self._reaction_distribution = parsed
                        # Extract emojis for backward compatibility
                        self.reaction_types = [
                            item.get('emoji', 'unknown') for item in parsed
                        ]
                else:
                    self.reaction_types = []
            except json.JSONDecodeError:
                self.reaction_types = []
        elif self.reaction_types is None:
            self.reaction_types = []

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'ChannelSettings':
        """Create instance from database row with proper handling"""
        data = {k: v for k, v in row.items() if k in cls.__annotations__}
        return cls(**data)

    def get_reaction_distribution(self) -> List[Dict[str, Any]]:
        """Get reaction distribution configuration"""
        if self._reaction_distribution:
            return self._reaction_distribution

        # Legacy format - create distribution from reaction_types
        if self.reaction_types and self.reactions_service_id:
            # Equal distribution among reaction types
            per_reaction = self.reactions_target // len(self.reaction_types) if self.reaction_types else 0
            return [
                {
                    'service_id': self.reactions_service_id,
                    'emoji': emoji,
                    'quantity': per_reaction
                }
                for emoji in self.reaction_types
            ]

        return []

    def get_effective_service_id(self, service_type: ServiceType) -> Optional[int]:
        """Get effective service ID for type, considering overrides"""
        if self._service_overrides and service_type in self._service_overrides:
            return self._service_overrides[service_type]

        if service_type == ServiceType.VIEWS:
            return self.views_service_id
        elif service_type == ServiceType.REACTIONS:
            return self.reactions_service_id
        elif service_type == ServiceType.REPOSTS:
            return self.reposts_service_id

        return None

    def should_randomize(self, service_type: ServiceType) -> bool:
        """Check if service type should be randomized"""
        if service_type == ServiceType.REACTIONS:
            return self.randomize_reactions
        elif service_type == ServiceType.REPOSTS:
            return self.randomize_reposts
        return False

    def get_target_quantity(self, service_type: ServiceType) -> int:
        """Get target quantity for service type"""
        if service_type == ServiceType.VIEWS:
            return self.views_target
        elif service_type == ServiceType.REACTIONS:
            return self.reactions_target
        elif service_type == ServiceType.REPOSTS:
            return self.reposts_target
        return 0


@dataclass
class Post:
    """Enhanced post model"""
    id: int
    channel_id: int
    post_id: int
    post_url: Optional[str]
    published_at: datetime
    processed_at: Optional[datetime]
    status: str
    created_at: datetime

    # Additional metadata
    _metadata: Optional[Dict[str, Any]] = field(default=None, init=False)
    _error_info: Optional[Dict[str, Any]] = field(default=None, init=False)

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Post':
        """Create instance from database row"""
        return cls(**{k: v for k, v in row.items() if k in cls.__annotations__})

    def is_new(self) -> bool:
        """Check if post is new"""
        return self.status == PostStatus.NEW

    def is_processing(self) -> bool:
        """Check if post is being processed"""
        return self.status == PostStatus.PROCESSING

    def is_completed(self) -> bool:
        """Check if post processing is completed"""
        return self.status == PostStatus.COMPLETED

    def is_failed(self) -> bool:
        """Check if post processing failed"""
        return self.status == PostStatus.FAILED

    def can_retry(self) -> bool:
        """Check if post can be retried"""
        if not self.is_failed():
            return False

        # Don't retry posts older than 48 hours
        age = datetime.utcnow() - self.published_at
        return age.total_seconds() < 48 * 3600

    def get_age_hours(self) -> float:
        """Get post age in hours"""
        age = datetime.utcnow() - self.published_at
        return age.total_seconds() / 3600


@dataclass
class Order:
    """Enhanced order model"""
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

    # Additional fields
    _actual_quantity: Optional[int] = field(default=None, init=False)
    _cost: Optional[Decimal] = field(default=None, init=False)
    _error_info: Optional[Dict[str, Any]] = field(default=None, init=False)

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Order':
        """Create instance from database row"""
        return cls(**{k: v for k, v in row.items() if k in cls.__annotations__})

    def is_active(self) -> bool:
        """Check if order is active"""
        return self.status in (OrderStatus.PENDING, OrderStatus.IN_PROGRESS)

    def is_completed(self) -> bool:
        """Check if order is completed"""
        return self.status == OrderStatus.COMPLETED

    def is_failed(self) -> bool:
        """Check if order failed"""
        return self.status in (OrderStatus.FAILED, OrderStatus.CANCELLED)

    def get_duration_minutes(self) -> Optional[int]:
        """Get order duration in minutes"""
        if not self.started_at or not self.completed_at:
            return None
        duration = self.completed_at - self.started_at
        return int(duration.total_seconds() / 60)

    def get_actual_quantity(self) -> int:
        """Get actual delivered quantity"""
        return self._actual_quantity or self.total_quantity

    def set_actual_quantity(self, quantity: int):
        """Set actual delivered quantity"""
        self._actual_quantity = quantity

    def get_cost(self) -> Optional[Decimal]:
        """Get order cost"""
        return self._cost

    def set_cost(self, cost: Decimal):
        """Set order cost"""
        self._cost = cost


@dataclass
class OrderPortion:
    """Enhanced order portion model"""
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

    # Additional tracking
    _completed_runs: int = field(default=0, init=False)
    _remaining_runs: Optional[int] = field(default=None, init=False)

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'OrderPortion':
        """Create instance from database row"""
        return cls(**{k: v for k, v in row.items() if k in cls.__annotations__})

    def total_quantity(self) -> int:
        """Calculate total quantity for this portion"""
        return self.quantity_per_run * self.runs

    def is_ready(self) -> bool:
        """Check if portion is ready to execute"""
        if self.status != PortionStatus.WAITING:
            return False
        if not self.scheduled_at:
            return True
        return datetime.utcnow() >= self.scheduled_at

    def is_running(self) -> bool:
        """Check if portion is running"""
        return self.status == PortionStatus.RUNNING

    def is_completed(self) -> bool:
        """Check if portion is completed"""
        return self.status == PortionStatus.COMPLETED

    def get_progress_percent(self) -> float:
        """Get completion progress percentage"""
        if self.runs == 0:
            return 100.0
        completed = self._completed_runs or 0
        return (completed / self.runs) * 100

    def get_remaining_quantity(self) -> int:
        """Get remaining quantity to deliver"""
        completed = self._completed_runs or 0
        return (self.runs - completed) * self.quantity_per_run


@dataclass
class Service:
    """Enhanced service model"""
    id: int
    nakrutka_id: int
    service_name: str
    service_type: str
    price_per_1000: Decimal
    min_quantity: int
    max_quantity: int
    is_active: bool
    updated_at: datetime

    # Additional metadata
    _features: Optional[Dict[str, Any]] = field(default=None, init=False)
    _performance_score: Optional[float] = field(default=None, init=False)

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Service':
        """Create instance from database row"""
        data = {k: v for k, v in row.items() if k in cls.__annotations__}
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

    def adjust_quantity(self, quantity: int) -> int:
        """Adjust quantity to fit within service limits"""
        if quantity < self.min_quantity:
            return self.min_quantity
        elif quantity > self.max_quantity:
            return self.max_quantity
        return quantity

    def get_emoji(self) -> str:
        """Extract emoji from service name"""
        import re
        emoji_pattern = r'[^\w\s,.\-()]{1,2}'
        emojis = re.findall(emoji_pattern, self.service_name)
        return emojis[0] if emojis else ''

    def is_premium(self) -> bool:
        """Check if service is premium"""
        return 'premium' in self.service_name.lower()

    def has_refill(self) -> bool:
        """Check if service has refill guarantee"""
        return 'R' in self.service_name and bool(re.search(r'R\d+', self.service_name))

    def get_refill_days(self) -> Optional[int]:
        """Get refill guarantee days"""
        import re
        match = re.search(r'R(\d+)', self.service_name)
        return int(match.group(1)) if match else None

    def get_country(self) -> Optional[str]:
        """Extract country from service name"""
        countries = {
            'UA': 'Ukraine', 'США': 'USA', 'СНГ': 'CIS',
            'Казахстан': 'Kazakhstan', 'EN': 'English'
        }
        for code, name in countries.items():
            if code in self.service_name:
                return name
        return None

    def set_performance_score(self, score: float):
        """Set service performance score"""
        self._performance_score = max(0.0, min(1.0, score))

    def get_performance_score(self) -> float:
        """Get service performance score"""
        return self._performance_score or 0.5


@dataclass
class PortionTemplate:
    """Enhanced portion template model"""
    id: int
    channel_id: int
    service_type: str
    portion_number: int
    quantity_percent: Decimal
    runs_formula: str
    interval_minutes: int
    start_delay_minutes: int
    created_at: datetime

    # Additional optimization fields
    _optimal_quantity_per_run: Optional[int] = field(default=None, init=False)
    _adjustment_factor: float = field(default=1.0, init=False)

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'PortionTemplate':
        """Create instance from database row"""
        data = {k: v for k, v in row.items() if k in cls.__annotations__}
        # Ensure quantity_percent is Decimal
        if 'quantity_percent' in data and not isinstance(data['quantity_percent'], Decimal):
            data['quantity_percent'] = Decimal(str(data['quantity_percent']))
        return cls(**data)

    def calculate_quantity(self, total: int) -> int:
        """Calculate quantity for this portion with adjustment"""
        base_quantity = int(total * float(self.quantity_percent) / 100)
        return int(base_quantity * self._adjustment_factor)

    def calculate_runs(self, portion_quantity: int) -> Tuple[int, int]:
        """Calculate quantity per run and number of runs"""
        if self.runs_formula.startswith('quantity/'):
            # Formula like "quantity/134"
            divisor = int(self.runs_formula.replace('quantity/', ''))
            quantity_per_run = divisor
            runs = max(1, (portion_quantity + divisor - 1) // divisor)  # Ceiling division
        else:
            # Fixed runs
            runs = max(1, int(self.runs_formula) if self.runs_formula.isdigit() else 1)
            quantity_per_run = max(1, portion_quantity // runs)

        # Apply optimal quantity if set
        if self._optimal_quantity_per_run:
            quantity_per_run = self._optimal_quantity_per_run
            runs = max(1, (portion_quantity + quantity_per_run - 1) // quantity_per_run)

        return quantity_per_run, runs

    def is_fast_portion(self) -> bool:
        """Check if this is a fast delivery portion"""
        return self.start_delay_minutes < 60

    def set_adjustment_factor(self, factor: float):
        """Set adjustment factor for dynamic optimization"""
        self._adjustment_factor = max(0.5, min(1.5, factor))


@dataclass
class ChannelReactionService:
    """Enhanced channel reaction service model"""
    id: int
    channel_id: int
    service_id: int
    emoji: str
    target_quantity: int
    created_at: datetime

    # Additional fields for better management
    _priority: int = field(default=1, init=False)
    _actual_service: Optional[Service] = field(default=None, init=False)

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'ChannelReactionService':
        """Create instance from database row"""
        return cls(**{k: v for k, v in row.items() if k in cls.__annotations__})

    def get_proportion(self, total_reactions: int) -> float:
        """Get proportion of this reaction type"""
        if total_reactions == 0:
            return 0.0
        return self.target_quantity / total_reactions

    def calculate_quantity(self, total_reactions: int) -> int:
        """Calculate quantity based on proportion"""
        return int(total_reactions * self.get_proportion(total_reactions))

    def set_service(self, service: Service):
        """Set associated service object"""
        self._actual_service = service

    def get_service(self) -> Optional[Service]:
        """Get associated service object"""
        return self._actual_service

    def is_available(self) -> bool:
        """Check if service is available"""
        return self._actual_service is not None and self._actual_service.is_active


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
        return cls(**{k: v for k, v in row.items() if k in cls.__annotations__})

    def mask_key(self) -> str:
        """Get masked version of API key"""
        if len(self.api_key) <= 8:
            return '*' * len(self.api_key)
        return f"{self.api_key[:4]}...{self.api_key[-4:]}"


@dataclass
class Log:
    """Enhanced log entry model"""
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
        return cls(**{k: v for k, v in row.items() if k in cls.__annotations__})

    def get_error_type(self) -> Optional[str]:
        """Extract error type from context"""
        if isinstance(self.context, dict):
            return self.context.get('error_type')
        return None

    def get_channel_id(self) -> Optional[int]:
        """Extract channel ID from context"""
        if isinstance(self.context, dict):
            return self.context.get('channel_id')
        return None


# ============ COMPOSITE MODELS ============

@dataclass
class ChannelConfig:
    """Complete channel configuration"""
    channel: Channel
    settings: ChannelSettings
    reaction_services: List[ChannelReactionService]
    portion_templates: Dict[str, List[PortionTemplate]]

    def get_service_targets(self) -> Dict[ServiceType, int]:
        """Get all service targets"""
        return {
            ServiceType.VIEWS: self.settings.views_target,
            ServiceType.REACTIONS: self.settings.reactions_target,
            ServiceType.REPOSTS: self.settings.reposts_target
        }

    def is_service_enabled(self, service_type: ServiceType) -> bool:
        """Check if service type is enabled"""
        return self.settings.get_target_quantity(service_type) > 0

    def get_reaction_distribution(self) -> List[Dict[str, Any]]:
        """Get reaction distribution with service info"""
        distribution = []

        total_target = self.settings.reactions_target
        if total_target == 0:
            return distribution

        # Use reaction services if available
        if self.reaction_services:
            total_configured = sum(rs.target_quantity for rs in self.reaction_services)

            for rs in self.reaction_services:
                # Calculate proportional quantity
                if total_configured > 0:
                    proportion = rs.target_quantity / total_configured
                    quantity = int(total_target * proportion)
                else:
                    quantity = total_target // len(self.reaction_services)

                distribution.append({
                    'service_id': rs.service_id,
                    'emoji': rs.emoji,
                    'quantity': quantity,
                    'priority': rs._priority
                })
        else:
            # Fallback to settings
            distribution = self.settings.get_reaction_distribution()

        return distribution


@dataclass
class OrderRequest:
    """Enhanced order request with validation"""
    post_id: int
    service_type: ServiceType
    service_id: int
    total_quantity: int
    portions: List[Dict[str, Any]]
    start_delay_minutes: int = 0

    # Validation results
    _validation_errors: List[str] = field(default_factory=list, init=False)
    _adjusted_quantity: Optional[int] = field(default=None, init=False)

    def is_valid(self) -> bool:
        """Check if request is valid"""
        return len(self._validation_errors) == 0

    def add_validation_error(self, error: str):
        """Add validation error"""
        self._validation_errors.append(error)

    def get_validation_errors(self) -> List[str]:
        """Get all validation errors"""
        return self._validation_errors

    def set_adjusted_quantity(self, quantity: int):
        """Set adjusted quantity after validation"""
        self._adjusted_quantity = quantity

    def get_final_quantity(self) -> int:
        """Get final quantity to use"""
        return self._adjusted_quantity or self.total_quantity


@dataclass
class ProcessingResult:
    """Result of post processing"""
    post_id: int
    success: bool
    orders_created: Dict[ServiceType, int] = field(default_factory=dict)
    total_cost: Decimal = field(default=Decimal('0'))
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    processing_time: float = 0.0

    def add_order(self, service_type: ServiceType, order_id: int, cost: Decimal):
        """Add successful order"""
        self.orders_created[service_type] = order_id
        self.total_cost += cost

    def add_error(self, error: str):
        """Add error"""
        self.errors.append(error)
        self.success = False

    def add_warning(self, warning: str):
        """Add warning"""
        self.warnings.append(warning)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging"""
        return {
            'post_id': self.post_id,
            'success': self.success,
            'orders_created': {k.value: v for k, v in self.orders_created.items()},
            'total_cost': float(self.total_cost),
            'errors': self.errors,
            'warnings': self.warnings,
            'processing_time': self.processing_time
        }


# ============ STATISTICS MODELS ============

@dataclass
class ChannelStats:
    """Enhanced channel statistics"""
    channel_id: int
    channel_username: str
    total_posts: int = 0
    completed_posts: int = 0
    processing_posts: int = 0
    failed_posts: int = 0
    total_views: int = 0
    total_reactions: int = 0
    total_reposts: int = 0
    total_cost: Decimal = field(default=Decimal('0'))
    success_rate: float = 0.0
    avg_processing_time: float = 0.0
    last_post_date: Optional[datetime] = None

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'ChannelStats':
        """Create instance from database row"""
        data = {k: v for k, v in row.items() if k in cls.__annotations__}

        # Calculate success rate
        if 'total_posts' in data and 'completed_posts' in data:
            total = data.get('total_posts', 0)
            completed = data.get('completed_posts', 0)
            data['success_rate'] = (completed / total * 100) if total > 0 else 0.0

        # Ensure Decimal for cost
        if 'total_cost' in data and not isinstance(data['total_cost'], Decimal):
            data['total_cost'] = Decimal(str(data['total_cost']))

        return cls(**data)

    def get_health_status(self) -> str:
        """Get channel health status"""
        if self.success_rate >= 95:
            return "excellent"
        elif self.success_rate >= 80:
            return "good"
        elif self.success_rate >= 60:
            return "fair"
        else:
            return "poor"

    def get_activity_level(self) -> str:
        """Get channel activity level"""
        if not self.last_post_date:
            return "inactive"

        days_since_last = (datetime.utcnow() - self.last_post_date).days
        if days_since_last == 0:
            return "very_active"
        elif days_since_last <= 1:
            return "active"
        elif days_since_last <= 7:
            return "moderate"
        else:
            return "low"


@dataclass
class ServiceStats:
    """Service performance statistics"""
    service_id: int
    service_name: str
    service_type: ServiceType
    total_orders: int = 0
    completed_orders: int = 0
    failed_orders: int = 0
    total_quantity: int = 0
    total_cost: Decimal = field(default=Decimal('0'))
    success_rate: float = 0.0
    avg_completion_time: float = 0.0
    last_used: Optional[datetime] = None

    @classmethod
    def from_db_data(cls, service: Service, stats: Dict[str, Any]) -> 'ServiceStats':
        """Create from service and stats data"""
        return cls(
            service_id=service.nakrutka_id,
            service_name=service.service_name,
            service_type=ServiceType(service.service_type),
            total_orders=stats.get('total_orders', 0),
            completed_orders=stats.get('completed_orders', 0),
            failed_orders=stats.get('failed_orders', 0),
            total_quantity=stats.get('total_quantity', 0),
            total_cost=Decimal(str(stats.get('total_cost', 0))),
            success_rate=stats.get('success_rate', 0.0),
            avg_completion_time=stats.get('avg_completion_time', 0.0),
            last_used=stats.get('last_used')
        )

    def calculate_score(self) -> float:
        """Calculate service performance score (0-1)"""
        # Weighted scoring
        success_weight = 0.6
        cost_weight = 0.3
        time_weight = 0.1

        # Success rate score (0-1)
        success_score = self.success_rate / 100.0

        # Cost efficiency score (inverse, lower is better)
        # Normalize against average price
        avg_price_per_1000 = 0.05  # Example average
        if self.total_quantity > 0:
            actual_price = float(self.total_cost) / (self.total_quantity / 1000)
            cost_score = min(1.0, avg_price_per_1000 / actual_price) if actual_price > 0 else 0.5
        else:
            cost_score = 0.5

        # Time score (faster is better)
        # Normalize against 24 hours
        if self.avg_completion_time > 0:
            time_score = min(1.0, 60 / self.avg_completion_time)  # 60 mins is ideal
        else:
            time_score = 0.5

        # Calculate weighted score
        score = (
            success_weight * success_score +
            cost_weight * cost_score +
            time_weight * time_score
        )

        return round(score, 3)


# Type aliases for clarity
ChannelID = int
PostID = int
OrderID = int
ServiceID = int
NakrutkaServiceID = int