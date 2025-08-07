"""
Post repository for database operations
"""
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from src.database.connection import db
from src.database.models import Post, PostStatus
from src.utils.logger import get_logger, LoggerMixin, metrics

logger = get_logger(__name__)


class PostRepository(LoggerMixin):
    """Repository for post operations"""

    async def create_post(self, post_data: Dict[str, Any]) -> Post:
        """Create new post"""
        query = """
                INSERT INTO posts (channel_id, message_id, content, media_type, status)
                VALUES ($1, $2, $3, $4, $5) ON CONFLICT (channel_id, message_id) DO NOTHING
            RETURNING * \
                """

        row = await db.fetchrow(
            query,
            post_data["channel_id"],
            post_data["message_id"],
            post_data.get("content"),
            post_data.get("media_type"),
            PostStatus.NEW
        )

        if row:
            post = self._row_to_post(row)
            self.log_info(
                "New post created",
                post_id=post.id,
                channel_id=post.channel_id,
                message_id=post.message_id
            )
            metrics.log_post_detected(post.channel_id, post.message_id)
            return post
        else:
            # Post already exists
            self.log_debug(
                "Post already exists",
                channel_id=post_data["channel_id"],
                message_id=post_data["message_id"]
            )
            return await self.get_post_by_message_id(
                post_data["channel_id"],
                post_data["message_id"]
            )

    async def bulk_create_posts(self, posts_data: List[Dict[str, Any]]) -> List[Post]:
        """Create multiple posts at once"""
        if not posts_data:
            return []

        # Prepare values for bulk insert
        values = []
        for post in posts_data:
            values.append((
                post["channel_id"],
                post["message_id"],
                post.get("content"),
                post.get("media_type"),
                PostStatus.NEW
            ))

        query = """
                INSERT INTO posts (channel_id, message_id, content, media_type, status)
                VALUES ($1, $2, $3, $4, $5) ON CONFLICT (channel_id, message_id) DO NOTHING
            RETURNING * \
                """

        # Execute bulk insert
        created_posts = []
        async with db.transaction():
            for value in values:
                row = await db.fetchrow(query, *value)
                if row:
                    created_posts.append(self._row_to_post(row))

        self.log_info(f"Bulk created {len(created_posts)} new posts")

        for post in created_posts:
            metrics.log_post_detected(post.channel_id, post.message_id)

        return created_posts

    async def get_post(self, post_id: int) -> Optional[Post]:
        """Get post by ID"""
        query = "SELECT * FROM posts WHERE id = $1"
        row = await db.fetchrow(query, post_id)

        if row:
            return self._row_to_post(row)
        return None

    async def get_post_by_message_id(
            self,
            channel_id: int,
            message_id: int
    ) -> Optional[Post]:
        """Get post by channel and message ID"""
        query = """
                SELECT * \
                FROM posts
                WHERE channel_id = $1 \
                  AND message_id = $2 \
                """
        row = await db.fetchrow(query, channel_id, message_id)

        if row:
            return self._row_to_post(row)
        return None

    async def get_new_posts(self, limit: int = 10) -> List[Post]:
        """Get posts with 'new' status"""
        query = """
                SELECT * \
                FROM posts
                WHERE status = $1
                ORDER BY detected_at ASC
                    LIMIT $2 \
                """
        rows = await db.fetch(query, PostStatus.NEW, limit)

        posts = [self._row_to_post(row) for row in rows]
        self.log_debug(f"Found {len(posts)} new posts")
        return posts

    async def get_posts_by_status(
            self,
            status: PostStatus,
            channel_id: Optional[int] = None,
            limit: int = 100
    ) -> List[Post]:
        """Get posts by status and optional channel"""
        if channel_id:
            query = """
                    SELECT * \
                    FROM posts
                    WHERE status = $1 \
                      AND channel_id = $2
                    ORDER BY detected_at DESC
                        LIMIT $3 \
                    """
            rows = await db.fetch(query, status, channel_id, limit)
        else:
            query = """
                    SELECT * \
                    FROM posts
                    WHERE status = $1
                    ORDER BY detected_at DESC
                        LIMIT $2 \
                    """
            rows = await db.fetch(query, status, limit)

        return [self._row_to_post(row) for row in rows]

    async def get_recent_posts(
            self,
            channel_id: int,
            hours: int = 24,
            limit: int = 100
    ) -> List[Post]:
        """Get recent posts from channel"""
        query = """
            SELECT * FROM posts 
            WHERE channel_id = $1 
                AND detected_at > NOW() - INTERVAL '%s hours'
            ORDER BY message_id DESC
            LIMIT $2
        """ % hours

        rows = await db.fetch(query, channel_id, limit)
        return [self._row_to_post(row) for row in rows]

    async def get_last_message_id(self, channel_id: int) -> Optional[int]:
        """Get last message ID for channel"""
        query = """
                SELECT MAX(message_id) as last_id
                FROM posts
                WHERE channel_id = $1 \
                """
        result = await db.fetchval(query, channel_id)
        return result

    async def update_status(
            self,
            post_id: int,
            status: PostStatus,
            processed_at: bool = False
    ):
        """Update post status"""
        if processed_at and status in [PostStatus.COMPLETED, PostStatus.FAILED]:
            query = """
                    UPDATE posts
                    SET status       = $2, \
                        processed_at = NOW()
                    WHERE id = $1 \
                    """
        else:
            query = """
                    UPDATE posts
                    SET status = $2
                    WHERE id = $1 \
                    """

        await db.execute(query, post_id, status)

        self.log_info(
            "Post status updated",
            post_id=post_id,
            status=status
        )

    async def bulk_update_status(
            self,
            post_ids: List[int],
            status: PostStatus
    ):
        """Update status for multiple posts"""
        if not post_ids:
            return

        query = """
                UPDATE posts
                SET status = $1
                WHERE id = ANY ($2::int[]) \
                """

        await db.execute(query, status, post_ids)

        self.log_info(
            f"Bulk updated {len(post_ids)} posts to status {status}"
        )

    async def update_stats(
            self,
            post_id: int,
            views: Optional[int] = None,
            reactions: Optional[int] = None,
            reposts: Optional[int] = None
    ):
        """Update post statistics"""
        updates = []
        params = [post_id]
        param_count = 1

        if views is not None:
            param_count += 1
            updates.append(f"views_count = views_count + ${param_count}")
            params.append(views)

        if reactions is not None:
            param_count += 1
            updates.append(f"reactions_count = reactions_count + ${param_count}")
            params.append(reactions)

        if reposts is not None:
            param_count += 1
            updates.append(f"reposts_count = reposts_count + ${param_count}")
            params.append(reposts)

        if updates:
            query = f"""
                UPDATE posts 
                SET {', '.join(updates)}
                WHERE id = $1
            """
            await db.execute(query, *params)

    async def check_post_exists(
            self,
            channel_id: int,
            message_id: int
    ) -> bool:
        """Check if post already exists"""
        query = """
                SELECT EXISTS(SELECT 1 \
                              FROM posts \
                              WHERE channel_id = $1 \
                                AND message_id = $2) \
                """
        return await db.fetchval(query, channel_id, message_id)

    async def get_processing_stats(self) -> Dict[str, int]:
        """Get statistics about post processing"""
        query = """
                SELECT status, \
                       COUNT(*) as count
                FROM posts
                WHERE detected_at > NOW() - INTERVAL '24 hours'
                GROUP BY status \
                """

        rows = await db.fetch(query)

        stats = {row["status"]: row["count"] for row in rows}

        # Add totals
        stats["total"] = sum(stats.values())
        stats["success_rate"] = (
            stats.get(PostStatus.COMPLETED, 0) / stats["total"] * 100
            if stats["total"] > 0 else 0
        )

        self.log_info("Processing stats", stats=stats)
        return stats

    async def cleanup_old_posts(self, days: int = 30):
        """Delete old completed posts"""
        query = """
            DELETE FROM posts 
            WHERE status = $1 
                AND processed_at < NOW() - INTERVAL '%s days'
        """ % days

        result = await db.execute(query, PostStatus.COMPLETED)

        # Extract count from result
        count = int(result.split()[-1]) if result else 0

        self.log_info(f"Cleaned up {count} old posts")
        return count

    # ========== Helper Methods ==========

    def _row_to_post(self, row) -> Post:
        """Convert database row to Post model"""
        return Post(
            id=row["id"],
            channel_id=row["channel_id"],
            message_id=row["message_id"],
            content=row["content"],
            status=PostStatus(row["status"]),
            detected_at=row["detected_at"],
            processed_at=row.get("processed_at")
        )


# Global repository instance
post_repo = PostRepository()