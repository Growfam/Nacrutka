"""
Simple database queries
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from src.database.connection import db

logger = logging.getLogger(__name__)


class Queries:
    """Simple database queries wrapper"""

    def __init__(self):
        self.db = db

    # Channel queries
    async def get_active_channels(self) -> List[Dict[str, Any]]:
        """Get all active channels"""
        return await self.db.fetch("""
            SELECT * FROM channels 
            WHERE is_active = true
            ORDER BY id
        """)

    async def get_channel_config(self, channel_id: int) -> Optional[Dict[str, Any]]:
        """Get channel configuration"""
        row = await self.db.fetchrow("""
            SELECT config FROM channel_config 
            WHERE channel_id = $1
        """, channel_id)
        return dict(row) if row else None

    # Post queries
    async def create_post(self, channel_id: int, post_id: int, post_url: str) -> bool:
        """Create new post"""
        try:
            await self.db.execute("""
                INSERT INTO posts (channel_id, post_id, post_url)
                VALUES ($1, $2, $3)
                ON CONFLICT (channel_id, post_id) DO NOTHING
            """, channel_id, post_id, post_url)
            return True
        except Exception as e:
            logger.error(f"Failed to create post: {e}")
            return False

    async def get_channel_posts(self, channel_id: int, limit: int = 100) -> List[int]:
        """Get post IDs for channel"""
        rows = await self.db.fetch("""
            SELECT post_id FROM posts
            WHERE channel_id = $1
            ORDER BY created_at DESC
            LIMIT $2
        """, channel_id, limit)
        return [row['post_id'] for row in rows]

    # Order queries
    async def get_pending_orders(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get pending orders"""
        rows = await self.db.fetch("""
            SELECT * FROM ready_orders
            WHERE status = 'pending'
            ORDER BY created_at
            LIMIT $1
        """, limit)
        return [dict(row) for row in rows]

    async def update_order_status(
        self,
        order_id: int,
        status: str,
        nakrutka_order_id: Optional[str] = None
    ):
        """Update order status"""
        if nakrutka_order_id:
            await self.db.execute("""
                UPDATE ready_orders
                SET status = $1,
                    nakrutka_order_id = $2,
                    sent_at = $3
                WHERE id = $4
            """, status, nakrutka_order_id, datetime.utcnow(), order_id)
        else:
            await self.db.execute("""
                UPDATE ready_orders
                SET status = $1
                WHERE id = $2
            """, status, order_id)

    # Statistics
    async def get_stats(self) -> Dict[str, Any]:
        """Get basic statistics"""
        stats = await self.db.fetchrow("""
            SELECT 
                (SELECT COUNT(*) FROM channels WHERE is_active = true) as active_channels,
                (SELECT COUNT(*) FROM posts WHERE created_at > NOW() - INTERVAL '24 hours') as posts_24h,
                (SELECT COUNT(*) FROM ready_orders WHERE status = 'pending') as pending_orders,
                (SELECT COUNT(*) FROM ready_orders WHERE status = 'sent') as sent_orders,
                (SELECT COUNT(*) FROM ready_orders WHERE created_at > NOW() - INTERVAL '24 hours') as orders_24h
        """)
        return dict(stats) if stats else {}

    # Logs
    async def log(self, level: str, message: str, context: Dict[str, Any] = None):
        """Add log entry"""
        import json
        await self.db.execute("""
            INSERT INTO logs (level, message, context)
            VALUES ($1, $2, $3)
        """, level, message, json.dumps(context or {}))

    async def cleanup_logs(self, days: int = 7) -> int:
        """Remove old logs"""
        result = await self.db.execute("""
            DELETE FROM logs
            WHERE created_at < NOW() - INTERVAL '%s days'
        """ % days)

        # Parse affected rows
        if result and 'DELETE' in result:
            return int(result.split()[-1])
        return 0