"""
Centralized cache management for the bot
"""
from typing import Any, Dict, Optional, List, Set, Union
from datetime import datetime, timedelta
import json
import asyncio
from collections import defaultdict
import pickle
import hashlib

from src.utils.logger import get_logger

logger = get_logger(__name__)


class CacheEntry:
    """Single cache entry with metadata"""

    def __init__(self, key: str, value: Any, ttl: int = 300):
        self.key = key
        self.value = value
        self.created_at = datetime.utcnow()
        self.ttl = ttl  # seconds
        self.hits = 0
        self.last_accessed = self.created_at

    def is_expired(self) -> bool:
        """Check if entry is expired"""
        if self.ttl <= 0:
            return False  # No expiration

        age = (datetime.utcnow() - self.created_at).total_seconds()
        return age > self.ttl

    def access(self):
        """Record access to this entry"""
        self.hits += 1
        self.last_accessed = datetime.utcnow()

    def get_age(self) -> float:
        """Get age in seconds"""
        return (datetime.utcnow() - self.created_at).total_seconds()


class CacheManager:
    """
    Centralized cache manager with advanced features:
    - TTL support
    - LRU eviction
    - Statistics tracking
    - Namespace support
    - Batch operations
    - Memory limits
    """

    def __init__(
            self,
            default_ttl: int = 300,
            max_entries: int = 10000,
            max_memory_mb: int = 100,
            cleanup_interval: int = 60
    ):
        self.default_ttl = default_ttl
        self.max_entries = max_entries
        self.max_memory_mb = max_memory_mb
        self.cleanup_interval = cleanup_interval

        # Main cache storage
        self._cache: Dict[str, CacheEntry] = {}

        # Namespace tracking
        self._namespaces: Dict[str, Set[str]] = defaultdict(set)

        # Statistics
        self._stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'expirations': 0,
            'sets': 0,
            'deletes': 0
        }

        # Locks for thread safety
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None

        # Start cleanup task
        self._start_cleanup_task()

    def _start_cleanup_task(self):
        """Start background cleanup task"""
        if not self._cleanup_task or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def _cleanup_loop(self):
        """Background task to clean expired entries"""
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self.cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cache cleanup: {e}")

    async def get(
            self,
            key: str,
            default: Any = None,
            namespace: Optional[str] = None
    ) -> Any:
        """Get value from cache"""
        full_key = self._make_key(key, namespace)

        async with self._lock:
            entry = self._cache.get(full_key)

            if entry is None:
                self._stats['misses'] += 1
                return default

            if entry.is_expired():
                self._stats['expirations'] += 1
                del self._cache[full_key]
                self._remove_from_namespace(full_key, namespace)
                return default

            entry.access()
            self._stats['hits'] += 1
            return entry.value

    async def set(
            self,
            key: str,
            value: Any,
            ttl: Optional[int] = None,
            namespace: Optional[str] = None
    ) -> bool:
        """Set value in cache"""
        full_key = self._make_key(key, namespace)

        if ttl is None:
            ttl = self.default_ttl

        async with self._lock:
            # Check memory limit
            if await self._check_memory_limit():
                await self._evict_lru()

            # Check entry count limit
            if len(self._cache) >= self.max_entries:
                await self._evict_lru()

            # Store entry
            self._cache[full_key] = CacheEntry(full_key, value, ttl)
            self._add_to_namespace(full_key, namespace)
            self._stats['sets'] += 1

            return True

    async def delete(
            self,
            key: str,
            namespace: Optional[str] = None
    ) -> bool:
        """Delete value from cache"""
        full_key = self._make_key(key, namespace)

        async with self._lock:
            if full_key in self._cache:
                del self._cache[full_key]
                self._remove_from_namespace(full_key, namespace)
                self._stats['deletes'] += 1
                return True
            return False

    async def exists(
            self,
            key: str,
            namespace: Optional[str] = None
    ) -> bool:
        """Check if key exists and not expired"""
        full_key = self._make_key(key, namespace)

        async with self._lock:
            entry = self._cache.get(full_key)
            if entry and not entry.is_expired():
                return True
            return False

    async def get_many(
            self,
            keys: List[str],
            namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get multiple values at once"""
        result = {}

        for key in keys:
            value = await self.get(key, namespace=namespace)
            if value is not None:
                result[key] = value

        return result

    async def set_many(
            self,
            items: Dict[str, Any],
            ttl: Optional[int] = None,
            namespace: Optional[str] = None
    ) -> int:
        """Set multiple values at once"""
        count = 0

        for key, value in items.items():
            if await self.set(key, value, ttl=ttl, namespace=namespace):
                count += 1

        return count

    async def delete_many(
            self,
            keys: List[str],
            namespace: Optional[str] = None
    ) -> int:
        """Delete multiple keys at once"""
        count = 0

        for key in keys:
            if await self.delete(key, namespace=namespace):
                count += 1

        return count

    async def clear_namespace(self, namespace: str) -> int:
        """Clear all entries in a namespace"""
        async with self._lock:
            if namespace not in self._namespaces:
                return 0

            keys = list(self._namespaces[namespace])
            count = 0

            for key in keys:
                if key in self._cache:
                    del self._cache[key]
                    count += 1

            del self._namespaces[namespace]
            return count

    async def clear_all(self):
        """Clear entire cache"""
        async with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._namespaces.clear()
            self._stats['deletes'] += count

    async def cleanup_expired(self) -> int:
        """Remove all expired entries"""
        async with self._lock:
            expired_keys = []

            for key, entry in self._cache.items():
                if entry.is_expired():
                    expired_keys.append(key)

            for key in expired_keys:
                del self._cache[key]
                # Extract namespace from key
                namespace = self._extract_namespace(key)
                self._remove_from_namespace(key, namespace)
                self._stats['expirations'] += 1

            return len(expired_keys)

    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        async with self._lock:
            total_entries = len(self._cache)

            # Calculate hit rate
            total_requests = self._stats['hits'] + self._stats['misses']
            hit_rate = (
                self._stats['hits'] / total_requests * 100
                if total_requests > 0 else 0
            )

            # Get size distribution
            size_distribution = self._get_size_distribution()

            # Get age distribution
            age_distribution = self._get_age_distribution()

            return {
                'total_entries': total_entries,
                'hit_rate': hit_rate,
                'stats': self._stats.copy(),
                'namespaces': {ns: len(keys) for ns, keys in self._namespaces.items()},
                'size_distribution': size_distribution,
                'age_distribution': age_distribution,
                'memory_usage_mb': await self._estimate_memory_usage()
            }

    def _make_key(self, key: str, namespace: Optional[str] = None) -> str:
        """Create full key with namespace"""
        if namespace:
            return f"{namespace}:{key}"
        return key

    def _extract_namespace(self, full_key: str) -> Optional[str]:
        """Extract namespace from full key"""
        if ':' in full_key:
            return full_key.split(':', 1)[0]
        return None

    def _add_to_namespace(self, full_key: str, namespace: Optional[str]):
        """Add key to namespace tracking"""
        if namespace:
            self._namespaces[namespace].add(full_key)

    def _remove_from_namespace(self, full_key: str, namespace: Optional[str]):
        """Remove key from namespace tracking"""
        if namespace and namespace in self._namespaces:
            self._namespaces[namespace].discard(full_key)
            if not self._namespaces[namespace]:
                del self._namespaces[namespace]

    async def _evict_lru(self) -> int:
        """Evict least recently used entries"""
        if not self._cache:
            return 0

        # Sort by last accessed time
        entries = sorted(
            self._cache.items(),
            key=lambda x: x[1].last_accessed
        )

        # Evict 10% of entries
        evict_count = max(1, len(entries) // 10)
        evicted = 0

        for key, _ in entries[:evict_count]:
            del self._cache[key]
            namespace = self._extract_namespace(key)
            self._remove_from_namespace(key, namespace)
            evicted += 1
            self._stats['evictions'] += 1

        return evicted

    async def _check_memory_limit(self) -> bool:
        """Check if memory limit is exceeded"""
        memory_usage = await self._estimate_memory_usage()
        return memory_usage > self.max_memory_mb

    async def _estimate_memory_usage(self) -> float:
        """Estimate memory usage in MB"""
        try:
            # Serialize a sample of entries to estimate size
            if not self._cache:
                return 0.0

            sample_size = min(100, len(self._cache))
            sample_keys = list(self._cache.keys())[:sample_size]

            total_size = 0
            for key in sample_keys:
                entry = self._cache[key]
                # Estimate size of serialized entry
                serialized = pickle.dumps(entry.value)
                total_size += len(serialized) + len(key)

            # Extrapolate to full cache
            if sample_size > 0:
                avg_size = total_size / sample_size
                estimated_total = avg_size * len(self._cache)
                return estimated_total / (1024 * 1024)  # Convert to MB

            return 0.0

        except Exception as e:
            logger.error(f"Error estimating memory usage: {e}")
            return 0.0

    def _get_size_distribution(self) -> Dict[str, int]:
        """Get distribution of entry sizes"""
        distribution = {
            'tiny': 0,  # < 100 bytes
            'small': 0,  # < 1KB
            'medium': 0,  # < 10KB
            'large': 0,  # < 100KB
            'huge': 0  # >= 100KB
        }

        for entry in self._cache.values():
            try:
                size = len(pickle.dumps(entry.value))

                if size < 100:
                    distribution['tiny'] += 1
                elif size < 1024:
                    distribution['small'] += 1
                elif size < 10240:
                    distribution['medium'] += 1
                elif size < 102400:
                    distribution['large'] += 1
                else:
                    distribution['huge'] += 1
            except:
                pass

        return distribution

    def _get_age_distribution(self) -> Dict[str, int]:
        """Get distribution of entry ages"""
        distribution = {
            'fresh': 0,  # < 1 minute
            'recent': 0,  # < 5 minutes
            'normal': 0,  # < 30 minutes
            'old': 0,  # < 1 hour
            'stale': 0  # >= 1 hour
        }

        for entry in self._cache.values():
            age = entry.get_age()

            if age < 60:
                distribution['fresh'] += 1
            elif age < 300:
                distribution['recent'] += 1
            elif age < 1800:
                distribution['normal'] += 1
            elif age < 3600:
                distribution['old'] += 1
            else:
                distribution['stale'] += 1

        return distribution

    # Specialized cache methods for common use cases

    async def cache_service(self, service_id: int, service_data: Dict[str, Any]) -> bool:
        """Cache service information"""
        return await self.set(
            f"service:{service_id}",
            service_data,
            ttl=3600,  # 1 hour
            namespace="services"
        )

    async def get_cached_service(self, service_id: int) -> Optional[Dict[str, Any]]:
        """Get cached service information"""
        return await self.get(
            f"service:{service_id}",
            namespace="services"
        )

    async def cache_channel_posts(
            self,
            channel_id: int,
            post_ids: List[int],
            ttl: int = 300
    ) -> bool:
        """Cache channel posts"""
        return await self.set(
            f"posts:{channel_id}",
            post_ids,
            ttl=ttl,
            namespace="channels"
        )

    async def get_cached_channel_posts(self, channel_id: int) -> Optional[List[int]]:
        """Get cached channel posts"""
        return await self.get(
            f"posts:{channel_id}",
            namespace="channels"
        )

    async def cache_order_status(
            self,
            order_id: str,
            status: Dict[str, Any],
            ttl: int = 30
    ) -> bool:
        """Cache order status"""
        return await self.set(
            f"order:{order_id}",
            status,
            ttl=ttl,
            namespace="orders"
        )

    async def get_cached_order_status(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get cached order status"""
        return await self.get(
            f"order:{order_id}",
            namespace="orders"
        )

    def create_key_hash(self, *args) -> str:
        """Create hash key from multiple arguments"""
        combined = ":".join(str(arg) for arg in args)
        return hashlib.md5(combined.encode()).hexdigest()

    async def shutdown(self):
        """Shutdown cache manager"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        await self.clear_all()
        logger.info("Cache manager shutdown complete")


# Global cache instance
_cache_instance: Optional[CacheManager] = None


def get_cache_manager() -> CacheManager:
    """Get global cache manager instance"""
    global _cache_instance

    if _cache_instance is None:
        _cache_instance = CacheManager()

    return _cache_instance


async def shutdown_cache():
    """Shutdown global cache"""
    global _cache_instance

    if _cache_instance:
        await _cache_instance.shutdown()
        _cache_instance = None