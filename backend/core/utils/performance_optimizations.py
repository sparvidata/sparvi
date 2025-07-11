import time
import threading
from functools import wraps
from typing import Dict, Any, Callable, Tuple
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# Cache storage with thread safety
class MetadataCache:
    """Thread-safe cache for metadata with timeout-based invalidation"""
    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        """Get the singleton instance"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def __init__(self):
        """Initialize the cache with thread-safe structures"""
        self.cache = {}
        self.cache_lock = threading.RLock()

    def get(self, key: str) -> Tuple[Any, bool]:
        """Get a value from the cache"""
        with self.cache_lock:
            if key in self.cache:
                value, timestamp, timeout = self.cache[key]
                if time.time() - timestamp < timeout:
                    # Cache hit - only log at DEBUG level occasionally
                    if hash(key) % 100 == 0:  # Log only 1% of cache hits
                        logger.debug(f"Metadata cache hit for {key}")
                    return value, True
                else:
                    # Expired
                    del self.cache[key]
                    logger.debug(f"Metadata cache expired for {key}")

        # Only log cache misses occasionally to reduce noise
        if hash(key) % 10 == 0:  # Log only 10% of cache misses
            logger.debug(f"Metadata cache miss for {key}")
        return None, False

    def set(self, key: str, value: Any, timeout_seconds: int) -> None:
        """Set a value in the cache with a timeout"""
        with self.cache_lock:
            self.cache[key] = (value, time.time(), timeout_seconds)
            # Only log cache sets occasionally
            if hash(key) % 20 == 0:  # Log only 5% of cache sets
                logger.debug(f"Cached metadata for {key} (timeout: {timeout_seconds}s)")

    def invalidate(self, prefix: str = None) -> int:
        """
        Invalidate all keys with a given prefix

        Args:
            prefix: Optional key prefix to match

        Returns:
            Number of keys invalidated
        """
        count = 0
        with self.cache_lock:
            if prefix:
                keys_to_delete = [k for k in self.cache.keys() if k.startswith(prefix)]
                for key in keys_to_delete:
                    del self.cache[key]
                    count += 1
            else:
                count = len(self.cache)
                self.cache.clear()
        return count

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the cache"""
        with self.cache_lock:
            return {
                "size": len(self.cache),
                "keys": list(self.cache.keys()),
                "memory_usage_bytes": sum(len(str(v)) for v in self.cache.values())
            }


# Create a global cache instance
metadata_cache = MetadataCache.get_instance()


# Cache decorator for any function
def cache_with_timeout(timeout_seconds: int = 300, prefix: str = None):
    """
    Cache function results with a timeout

    Args:
        timeout_seconds: Cache timeout in seconds
        prefix: Optional prefix for the cache key

    Returns:
        Decorated function
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create a cache key from function name and arguments
            func_name = prefix or func.__name__
            arg_str = ":".join(str(a) for a in args if not str(a).startswith("<"))
            kwarg_str = ":".join(f"{k}={v}" for k, v in sorted(kwargs.items()))
            key = f"{func_name}:{arg_str}:{kwarg_str}"

            # Try to get from cache
            value, hit = metadata_cache.get(key)
            if hit:
                logger.debug(f"Cache hit for {func_name}")
                return value

            # Cache miss, call the function
            logger.debug(f"Cache miss for {func_name}")
            start_time = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start_time

            # Cache the result
            metadata_cache.set(key, result, timeout_seconds)
            logger.debug(f"Cached result for {func_name} (took {duration:.3f}s)")

            return result

        return wrapper

    return decorator


# Cache for schema metadata (specifically connection metadata)
def cached_metadata(func: Callable) -> Callable:
    """
    Specialized cache decorator for metadata methods

    This decorator applies different cache timeouts based on the metadata type:
    - Tables metadata: 30 minutes
    - Column metadata: 15 minutes
    - Statistics metadata: 5 minutes

    Args:
        func: Function to decorate

    Returns:
        Decorated function
    """

    @wraps(func)
    def wrapper(self, connection_id: str, metadata_type: str = None, *args, **kwargs):
        # Determine cache timeout based on metadata type
        if metadata_type == "tables":
            timeout = 1800  # 30 minutes
        elif metadata_type == "columns":
            timeout = 900  # 15 minutes
        elif metadata_type == "statistics":
            timeout = 300  # 5 minutes
        else:
            timeout = 600  # 10 minutes default

        # Create a cache key
        key = f"metadata:{connection_id}:{metadata_type}"

        # Try to get from cache
        value, hit = metadata_cache.get(key)
        if hit:
            logger.debug(f"Metadata cache hit for {connection_id}:{metadata_type}")
            return value

        # Cache miss, call the original function
        logger.debug(f"Metadata cache miss for {connection_id}:{metadata_type}")
        start_time = time.time()
        result = func(self, connection_id, metadata_type, *args, **kwargs)
        duration = time.time() - start_time

        # Cache the result if it's not an error result
        if result and "error" not in result:
            metadata_cache.set(key, result, timeout)
            logger.debug(f"Cached metadata for {connection_id}:{metadata_type} (took {duration:.3f}s)")

        return result

    return wrapper


# Apply the cache decorators to the relevant methods
# Example application to storage service
def apply_caching_to_storage_service(MetadataStorageService):
    """Apply caching decorators to storage service methods"""
    try:
        # Cache the get_metadata method
        if hasattr(MetadataStorageService, 'get_metadata'):
            original_get_metadata = MetadataStorageService.get_metadata
            MetadataStorageService.get_metadata = cached_metadata(original_get_metadata)

        # Add invalidation methods
        def invalidate_metadata_cache(self, connection_id: str, metadata_type: str = None):
            """Invalidate metadata cache for a connection"""
            prefix = f"metadata:{connection_id}"
            if metadata_type:
                prefix += f":{metadata_type}"
            return metadata_cache.invalidate(prefix)

        MetadataStorageService.invalidate_metadata_cache = invalidate_metadata_cache

        # Override store methods to invalidate cache after storing
        if hasattr(MetadataStorageService, 'store_tables_metadata'):
            original_store_tables = MetadataStorageService.store_tables_metadata

            @wraps(original_store_tables)
            def wrapped_store_tables(self, connection_id, tables_metadata):
                result = original_store_tables(self, connection_id, tables_metadata)
                # Invalidate the cache for this connection's tables metadata
                self.invalidate_metadata_cache(connection_id, "tables")
                return result

            MetadataStorageService.store_tables_metadata = wrapped_store_tables

        if hasattr(MetadataStorageService, 'store_columns_metadata'):
            original_store_columns = MetadataStorageService.store_columns_metadata

            @wraps(original_store_columns)
            def wrapped_store_columns(self, connection_id, columns_by_table):
                result = original_store_columns(self, connection_id, columns_by_table)
                # Invalidate the cache for this connection's columns metadata
                self.invalidate_metadata_cache(connection_id, "columns")
                return result

            MetadataStorageService.store_columns_metadata = wrapped_store_columns

        if hasattr(MetadataStorageService, 'store_statistics_metadata'):
            original_store_statistics = MetadataStorageService.store_statistics_metadata

            @wraps(original_store_statistics)
            def wrapped_store_statistics(self, connection_id, stats_by_table):
                result = original_store_statistics(self, connection_id, stats_by_table)
                # Invalidate the cache for this connection's statistics metadata
                self.invalidate_metadata_cache(connection_id, "statistics")
                return result

            MetadataStorageService.store_statistics_metadata = wrapped_store_statistics

        logger.info("Applied caching to MetadataStorageService")
    except Exception as e:
        logger.warning(f"Could not apply caching to MetadataStorageService: {str(e)}")

    return MetadataStorageService


# Apply caching to SchemaChangeDetector
def apply_caching_to_schema_detector(SchemaChangeDetector):
    """Apply caching to schema change detector methods"""
    try:
        # Cache the compare_schemas method with a short timeout
        if hasattr(SchemaChangeDetector, 'compare_schemas'):
            original_compare = SchemaChangeDetector.compare_schemas
            SchemaChangeDetector.compare_schemas = cache_with_timeout(
                timeout_seconds=60, prefix="schema_compare"
            )(original_compare)

        # Cache detect_changes_for_connection with a very short timeout since it's important to get fresh results
        if hasattr(SchemaChangeDetector, 'detect_changes_for_connection'):
            original_detect = SchemaChangeDetector.detect_changes_for_connection
            SchemaChangeDetector.detect_changes_for_connection = cache_with_timeout(
                timeout_seconds=30, prefix="detect_changes"
            )(original_detect)

        # Only try to cache _extract_tables if it exists
        if hasattr(SchemaChangeDetector, '_extract_tables'):
            original_extract = SchemaChangeDetector._extract_tables
            SchemaChangeDetector._extract_tables = cache_with_timeout(
                timeout_seconds=30, prefix="extract_tables"
            )(original_extract)

        logger.info("Applied caching to SchemaChangeDetector")
    except Exception as e:
        logger.warning(f"Could not apply caching to SchemaChangeDetector: {str(e)}")

    return SchemaChangeDetector


# Apply caching to database connector methods for improved performance
def apply_caching_to_connector(SnowflakeConnector):
    """Apply caching to database connector methods"""
    try:
        # Cache get_tables with a longer timeout
        if hasattr(SnowflakeConnector, 'get_tables'):
            original_get_tables = SnowflakeConnector.get_tables
            SnowflakeConnector.get_tables = cache_with_timeout(
                timeout_seconds=600, prefix="snowflake_tables"
            )(original_get_tables)

        # Cache get_columns with a medium timeout
        if hasattr(SnowflakeConnector, 'get_columns'):
            original_get_columns = SnowflakeConnector.get_columns
            SnowflakeConnector.get_columns = cache_with_timeout(
                timeout_seconds=300, prefix="snowflake_columns"
            )(original_get_columns)

        # Cache get_primary_keys with a medium timeout
        if hasattr(SnowflakeConnector, 'get_primary_keys'):
            original_get_pk = SnowflakeConnector.get_primary_keys
            SnowflakeConnector.get_primary_keys = cache_with_timeout(
                timeout_seconds=300, prefix="snowflake_pk"
            )(original_get_pk)

        # Add cache invalidation method
        def invalidate_connector_cache(self):
            """Invalidate all connector caches"""
            invalidated = metadata_cache.invalidate("snowflake_")
            logger.info(f"Invalidated {invalidated} connector cache entries")
            return invalidated

        SnowflakeConnector.invalidate_connector_cache = invalidate_connector_cache

        logger.info("Applied caching to SnowflakeConnector")
    except Exception as e:
        logger.warning(f"Could not apply caching to SnowflakeConnector: {str(e)}")

    return SnowflakeConnector


# Optimized batch schema comparison function
@cache_with_timeout(timeout_seconds=60, prefix="batch_schema_compare")
def batch_compare_schemas(connection_id, current_tables, previous_tables,
                          batch_size=10, total_batches=5):
    """
    Compare schemas in batches for better performance

    Args:
        connection_id: Connection ID
        current_tables: Dictionary of current tables
        previous_tables: Dictionary of previous tables
        batch_size: Number of tables to process in each batch
        total_batches: Maximum number of batches to process

    Returns:
        List of detected changes
    """
    changes = []

    # Find added and removed tables first (these are quick operations)
    added_tables = set(current_tables.keys()) - set(previous_tables.keys())
    for table_name in added_tables:
        changes.append({
            "type": "table_added",
            "table": table_name,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    removed_tables = set(previous_tables.keys()) - set(current_tables.keys())
    for table_name in removed_tables:
        changes.append({
            "type": "table_removed",
            "table": table_name,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    # For detailed comparisons, only look at common tables and limit to batches
    common_tables = list(set(current_tables.keys()) & set(previous_tables.keys()))

    # Process only a subset of tables if there are too many
    if len(common_tables) > batch_size * total_batches:
        logger.info(f"Limiting schema comparison to {batch_size * total_batches} of {len(common_tables)} tables")
        import random
        # Prioritize important tables if we can identify them
        # Here we're just taking a random sample, but you could prioritize based on usage metrics
        common_tables = random.sample(common_tables, batch_size * total_batches)

    # Process in batches
    for batch_idx in range(min(total_batches, (len(common_tables) + batch_size - 1) // batch_size)):
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, len(common_tables))
        batch_tables = common_tables[start_idx:end_idx]

        logger.info(f"Processing schema comparison batch {batch_idx + 1}: tables {start_idx + 1}-{end_idx}")

        # Create detector instance
        try:
            from core.metadata.schema_change_detector import SchemaChangeDetector
            detector = SchemaChangeDetector()

            # Compare each table in the batch
            for table_name in batch_tables:
                try:
                    # Column changes
                    if hasattr(detector, '_compare_table_columns'):
                        column_changes = detector._compare_table_columns(
                            table_name,
                            current_tables[table_name].get("columns", []),
                            previous_tables[table_name].get("columns", [])
                        )
                        changes.extend(column_changes)

                    # Primary key changes
                    if hasattr(detector, '_compare_primary_keys'):
                        pk_changes = detector._compare_primary_keys(
                            table_name,
                            current_tables[table_name].get("primary_key", []),
                            previous_tables[table_name].get("primary_key", [])
                        )
                        changes.extend(pk_changes)

                    # Only process foreign keys and indexes for the first 2 batches
                    # as these are more expensive operations
                    if batch_idx < 2:
                        # Foreign key changes
                        if hasattr(detector, '_compare_foreign_keys'):
                            fk_changes = detector._compare_foreign_keys(
                                table_name,
                                current_tables[table_name].get("foreign_keys", []),
                                previous_tables[table_name].get("foreign_keys", [])
                            )
                            changes.extend(fk_changes)

                        # Index changes
                        if hasattr(detector, '_compare_indexes'):
                            index_changes = detector._compare_indexes(
                                table_name,
                                current_tables[table_name].get("indices", []),
                                previous_tables[table_name].get("indices", [])
                            )
                            changes.extend(index_changes)
                except Exception as table_error:
                    logger.warning(f"Error comparing table {table_name}: {str(table_error)}")
                    continue

        except ImportError:
            logger.warning("SchemaChangeDetector not available for batch comparison")
            break

    return changes


# Function to apply all caching optimizations
def apply_performance_optimizations():
    """Apply all performance optimizations to the schema change detection system"""
    try:
        logger.info("Starting to apply performance optimizations")

        # Initialize results dict
        results = {}

        # Try to import and optimize MetadataStorageService
        try:
            from core.metadata.storage_service import MetadataStorageService
            MetadataStorageService = apply_caching_to_storage_service(MetadataStorageService)
            results["MetadataStorageService"] = MetadataStorageService
            logger.info("Applied optimizations to MetadataStorageService")
        except ImportError as e:
            logger.warning(f"Could not import MetadataStorageService: {str(e)}")
        except Exception as e:
            logger.warning(f"Error optimizing MetadataStorageService: {str(e)}")

        # Try to import and optimize SchemaChangeDetector
        try:
            from core.metadata.schema_change_detector import SchemaChangeDetector
            SchemaChangeDetector = apply_caching_to_schema_detector(SchemaChangeDetector)
            results["SchemaChangeDetector"] = SchemaChangeDetector
            logger.info("Applied optimizations to SchemaChangeDetector")
        except ImportError as e:
            logger.warning(f"Could not import SchemaChangeDetector: {str(e)}")
        except Exception as e:
            logger.warning(f"Error optimizing SchemaChangeDetector: {str(e)}")

        # Try to import and optimize SnowflakeConnector
        try:
            from core.metadata.connectors import SnowflakeConnector
            SnowflakeConnector = apply_caching_to_connector(SnowflakeConnector)
            results["SnowflakeConnector"] = SnowflakeConnector
            logger.info("Applied optimizations to SnowflakeConnector")
        except ImportError as e:
            logger.warning(f"Could not import SnowflakeConnector: {str(e)}")
        except Exception as e:
            logger.warning(f"Error optimizing SnowflakeConnector: {str(e)}")

        logger.info(f"Performance optimizations applied successfully to {len(results)} components")
        return results

    except Exception as e:
        logger.error(f"Error applying performance optimizations: {str(e)}")
        return {}


# Only apply optimizations when specifically requested, not on import
def get_optimized_classes():
    """Get optimized classes - call this when you want to apply optimizations"""
    return apply_performance_optimizations()


# Don't run optimizations on import to avoid startup errors
logger.info("Performance optimizations module loaded - call get_optimized_classes() to apply optimizations")