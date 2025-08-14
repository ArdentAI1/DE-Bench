"""
Helper class for managing Databricks cluster cache using SQLite database, stored in the Environment/Databricks directory.
"""

import os
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any, Optional


class CacheManager:
    """
    A class to manage Databricks cluster cache using SQLite database, which is used to store cluster information.

    :param Optional[int] default_expiry_hours: Default expiry time for cached clusters, defaults to 1 hour.
    """

    def __init__(
        self,
        default_expiry_hours: Optional[int] = 1,
    ):
        self.default_expiry_hours: int = default_expiry_hours
        self.expiry_time: str = (datetime.now() + timedelta(hours=default_expiry_hours)).isoformat()
        self.db_path = self.validate_cache_directory_exists()
        self._init_database()
    
    @staticmethod
    def validate_cache_directory_exists() -> str:
        """
        Validate that the cache directory exists and is writable, returns the path to the SQLite database.

        :return: Path to the SQLite database file.
        :rtype: str
        """
        cwd = os.getcwd()
        cache_dir = os.path.join(cwd, "Environment", "Databricks")
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        return os.path.join(cache_dir, "cluster_cache.db")

    def _init_database(self) -> None:
        """
        Initialize the SQLite database with the required table structure.

        :rtype: None
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create clusters table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS clusters (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        cluster_id TEXT UNIQUE NOT NULL,
                        cluster_name TEXT,
                        host TEXT,
                        num_workers INTEGER,
                        status TEXT DEFAULT 'RUNNING',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        expiry_time TIMESTAMP NOT NULL,
                        last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        access_count INTEGER DEFAULT 1,
                        is_active BOOLEAN DEFAULT 1,
                        is_shared BOOLEAN DEFAULT 0
                    )
                """)
                
                # Create index for faster lookups
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cluster_id ON clusters(cluster_id)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_expiry_time ON clusters(expiry_time)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_is_active ON clusters(is_active)
                """)
                
                conn.commit()
                
        except sqlite3.Error as e:
            print(f"Warning: Could not initialize cache database: {e}")

    def _get_connection(self) -> sqlite3.Connection:
        """
        Get a database connection with proper configuration for concurrent access.

        :return: SQLite database connection.
        :rtype: sqlite3.Connection
        """
        conn = sqlite3.connect(
            self.db_path,
            timeout=30.0,  # 30 second timeout for busy database
            check_same_thread=False  # Allow cross-thread access
        )
        conn.row_factory = sqlite3.Row  # Enable dict-like access to rows
        
        # Configure for better concurrent access
        conn.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging for better concurrency
        conn.execute("PRAGMA synchronous=NORMAL")  # Balance between safety and performance
        conn.execute("PRAGMA cache_size=10000")  # Increase cache size
        conn.execute("PRAGMA temp_store=MEMORY")  # Store temp tables in memory
        
        return conn

    def load_cluster_cache(self) -> dict[str, Any]:
        """
        Load the most recent active cluster cache from SQLite database.

        :return: Dictionary containing cached cluster data.
        :rtype: dict[str, Any]
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Get the most recent active cluster
                cursor.execute("""
                    SELECT * FROM clusters 
                    WHERE is_active = 1 AND expiry_time > datetime('now')
                    ORDER BY created_at DESC 
                    LIMIT 1
                """)
                
                if row := cursor.fetchone():
                    return dict(row)
                return {}
                
        except sqlite3.Error as e:
            print(f"Warning: Could not load cluster cache: {e}")
            return {}

    def save_cluster_cache(self, cache_data: dict[str, Any]) -> None:
        """
        Save cluster cache to SQLite database with retry mechanism for lock handling.

        :param dict[str, Any] cache_data: Dictionary containing cluster data to save.
        :rtype: None
        """
        max_retries = 3
        retry_delay = 1.0  # seconds
        
        for attempt in range(max_retries):
            try:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    
                    # Use a transaction to make this atomic
                    cursor.execute("BEGIN TRANSACTION")
                    
                    try:
                        # Deactivate all existing clusters first
                        cursor.execute("UPDATE clusters SET is_active = 0")
                        
                        # Insert new cluster data with explicit is_active = 1
                        cursor.execute("""
                            INSERT OR REPLACE INTO clusters (
                                cluster_id, cluster_name, host, num_workers, status, created_at, expiry_time, is_shared, is_active
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                        """, (
                            cache_data.get("cluster_id"),
                            cache_data.get("cluster_name"),
                            cache_data.get("host"),
                            cache_data.get("num_workers", 1),
                            cache_data.get("status", "RUNNING"),
                            cache_data.get("created_at", datetime.now().isoformat()),
                            cache_data.get("expiry_time", self.expiry_time),
                            cache_data.get("is_shared", 0)
                        ))
                        
                        cursor.execute("COMMIT")
                        return  # Success, exit retry loop
                        
                    except Exception as e:
                        cursor.execute("ROLLBACK")
                        raise e
                    
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                    print(f"Database locked, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    print(f"Warning: Could not save cluster cache after {max_retries} attempts: {e}")
                    break
            except sqlite3.Error as e:
                print(f"Warning: Could not save cluster cache: {e}")
                break

    @staticmethod
    def is_cluster_cache_valid(cache_data: dict[str, Any]) -> bool:
        """
        Check if cached cluster data is still valid by comparing the expiry time.

        :param dict[str, Any] cache_data: Dictionary containing cluster data to check.
        :return: True if cache data is still valid, False otherwise.
        :rtype: bool
        """
        if not cache_data or "cluster_id" not in cache_data or "expiry_time" not in cache_data:
            return False

        try:
            expiry_time = datetime.fromisoformat(cache_data["expiry_time"])
            return datetime.now() < expiry_time
        except (ValueError, TypeError):
            return False
        
    def remove_terminated_cluster(self, cluster_id: str) -> None:
        """
        Remove a terminated cluster from the cache.

        :param str cluster_id: The ID of the cluster to remove.
        :rtype: None
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM clusters WHERE cluster_id = ?", (cluster_id,))
                conn.commit()
                print(f"Removed terminated cluster: {cluster_id}")
        except sqlite3.Error as e:
            print(f"Unable to remove terminated cluster {cluster_id} from cache: {e}")
    
    def clear_cluster_cache(self) -> None:
        """
        Clear the cluster cache (for testing or manual cleanup).

        :rtype: None
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM clusters")
                conn.commit()
                print("Cluster cache cleared")
        except sqlite3.Error as e:
            print(f"Warning: Could not clear cluster cache: {e}")

    def get_cached_cluster_info(self) -> Optional[dict[str, Any]]:
        """
        Get information about the currently cached cluster.

        :return: Dictionary containing cached cluster data.
        :rtype: Optional[dict[str, Any]]
        """
        cache_data = self.load_cluster_cache()
        if not cache_data:
            return None
        
        return {
            "cluster_id": cache_data.get("cluster_id"),
            "expiry_time": cache_data.get("expiry_time"),
            "created_at": cache_data.get("created_at"),
            "is_valid": self.is_cluster_cache_valid(cache_data),
            "access_count": cache_data.get("access_count", 0),
            "last_accessed": cache_data.get("last_accessed"),
            "is_shared": cache_data.get("is_shared", False)
        }

    def cache_new_cluster(self, cluster_id: str, cluster_config: Optional[dict[str, Any]] = None) -> None:
        """
        Cache a new cluster with expiry time and optional configuration details.

        :param str cluster_id: The ID of the cluster to cache.
        :param Optional[dict[str, Any]] cluster_config: Optional cluster configuration details.
        :rtype: None
        """
        expiry_time = datetime.now() + timedelta(hours=self.default_expiry_hours)
        
        cache_data = {
            "cluster_id": cluster_id,
            "expiry_time": expiry_time.isoformat(),
            "created_at": datetime.now().isoformat()
        }
        
        # Add cluster configuration if provided
        if cluster_config:
            cache_data.update({
                "cluster_name": cluster_config.get("cluster_name"),
                "host": cluster_config.get("host"),
                "node_type_id": cluster_config.get("node_type_id"),
                "spark_version": cluster_config.get("spark_version"),
                "num_workers": cluster_config.get("num_workers"),
                "autotermination_minutes": cluster_config.get("autotermination_minutes"),
                "is_shared": cluster_config.get("is_shared", False)
            })
        
        self.save_cluster_cache(cache_data)
        print(f"Cached cluster {cluster_id} until {expiry_time}")

    def update_cluster_access(self, cluster_id: str) -> None:
        """
        Update the last accessed time and access count for a cluster.

        :param str cluster_id: The ID of the cluster to update.
        :rtype: None
        """
        max_retries = 3
        retry_delay = 0.5  # seconds
        
        for attempt in range(max_retries):
            try:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE clusters 
                        SET last_accessed = datetime('now'), access_count = access_count + 1
                        WHERE cluster_id = ? AND is_active = 1
                    """, (cluster_id,))
                    conn.commit()
                    return  # Success, exit retry loop
                    
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                    print(f"Database locked during access update, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    print(f"Warning: Could not update cluster access after {max_retries} attempts: {e}")
                    break
            except sqlite3.Error as e:
                print(f"Warning: Could not update cluster access: {e}")
                break

    def get_all_clusters(self, where_clause: Optional[str] = None) -> list[dict[str, Any]]:
        """
        Get all clusters in the cache (for debugging and monitoring).

        :param Optional[str] where_clause: Optional WHERE clause to filter clusters.
        :return: List of dictionaries containing cluster data.
        :rtype: list[dict[str, Any]]
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                if where_clause:
                    query = f"SELECT * FROM clusters WHERE {where_clause} ORDER BY created_at DESC"
                else:
                    query = "SELECT * FROM clusters ORDER BY created_at DESC"
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                return [dict(row) for row in rows]
                
        except sqlite3.Error as e:
            print(f"Warning: Could not get all clusters: {e}")
            return []

    def cleanup_expired_clusters(self) -> int:
        """
        Remove expired clusters from the cache.

        :return: Number of expired clusters removed.
        :rtype: int
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    DELETE FROM clusters 
                    WHERE expiry_time <= datetime('now')
                """)
                deleted_count = cursor.rowcount
                conn.commit()
                return deleted_count
                
        except sqlite3.Error as e:
            print(f"Warning: Could not cleanup expired clusters: {e}")
            return 0

    def get_cache_statistics(self) -> dict[str, Any]:
        """
        Get statistics about the cache (for monitoring and debugging).

        :return: Dictionary containing cache statistics.
        :rtype: dict[str, Any]
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Get total clusters
                cursor.execute("SELECT COUNT(*) FROM clusters")
                total_clusters = cursor.fetchone()[0]
                
                # Get active clusters
                cursor.execute("SELECT COUNT(*) FROM clusters WHERE is_active = 1")
                active_clusters = cursor.fetchone()[0]
                
                # Get expired clusters
                cursor.execute("SELECT COUNT(*) FROM clusters WHERE expiry_time <= datetime('now')")
                expired_clusters = cursor.fetchone()[0]
                
                # Get total access count
                cursor.execute("SELECT SUM(access_count) FROM clusters")
                total_accesses = cursor.fetchone()[0] or 0

                # Get shared clusters
                cursor.execute("SELECT COUNT(*) FROM clusters WHERE is_shared = 1")
                shared_clusters = cursor.fetchone()[0] or 0
                
                return {
                    "total_clusters": total_clusters,
                    "active_clusters": active_clusters,
                    "expired_clusters": expired_clusters,
                    "total_accesses": total_accesses,
                    "shared_clusters": shared_clusters,
                    "cache_file_size": os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
                }
                
        except sqlite3.Error as e:
            print(f"Warning: Could not get cache statistics: {e}")
            return {}

    def optimize_database(self) -> None:
        """
        Optimize the database for better performance and cleanup WAL files.

        :rtype: None
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Run VACUUM to optimize the database
                cursor.execute("VACUUM")
                
                # Analyze tables for better query planning
                cursor.execute("ANALYZE")
                
                # Clean up WAL files
                cursor.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                
                conn.commit()
                print("Database optimized successfully")
                
        except sqlite3.Error as e:
            print(f"Warning: Could not optimize database: {e}")

    def get_database_info(self) -> dict[str, Any]:
        """
        Get information about the database file and WAL files.

        :return: Dictionary containing database information.
        :rtype: dict[str, Any]
        """
        info = {
            "database_path": self.db_path,
            "database_exists": os.path.exists(self.db_path),
            "database_size": 0,
            "wal_exists": False,
            "wal_size": 0,
            "shm_exists": False,
            "shm_size": 0
        }
        
        if info["database_exists"]:
            info["database_size"] = os.path.getsize(self.db_path)
            
            # Check for WAL file
            wal_path = f"{self.db_path}-wal"
            if os.path.exists(wal_path):
                info["wal_exists"] = True
                info["wal_size"] = os.path.getsize(wal_path)
            
            # Check for shared memory file
            shm_path = f"{self.db_path}-shm"
            if os.path.exists(shm_path):
                info["shm_exists"] = True
                info["shm_size"] = os.path.getsize(shm_path)
        
        return info
