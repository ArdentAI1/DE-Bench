"""
Helper class for managing Databricks ephemeral.json cache file, stored in the Environment/Databricks directory.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any, Optional


class CacheManager:
    """
    A class to manage Databricks ephemeral.json cache file, which is used to store cluster information.

    :param Optional[int] default_expiry_hours: Default expiry time for cached clusters, defaults to 1 hour.
    """

    def __init__(
        self,
        default_expiry_hours: Optional[int] = 1,
    ):
        self.default_expiry_hours: int = default_expiry_hours
        self.ephemeral_cache_file = self.validate_cache_directory_exists()
    
    @staticmethod
    def validate_cache_directory_exists() -> str:
        """
        Validate that the cache directory exists and is writable, returns the path to the cache directory.

        :return: Path to the cache directory.
        :rtype: str
        """
        cwd = os.getcwd()
        cache_dir = os.path.join(cwd, "Environment", "Databricks")
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        return os.path.join(cache_dir, "new_ephemeral.json")

    def load_cluster_cache(self) -> dict[str, Any]:
        """
        Load cluster cache from ephemeral.json file.

        :return: Dictionary containing cached cluster data.
        :rtype: dict[str, Any]
        """
        if not os.path.exists(self.ephemeral_cache_file):
            return {}

        try:
            with open(self.ephemeral_cache_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}

    def save_cluster_cache(self, cache_data: dict[str, Any]) -> None:
        """
        Save cluster cache to ephemeral.json

        :param dict[str, Any] cache_data: Dictionary containing cluster data to save.
        :rtype: None
        """
        os.makedirs(os.path.dirname(self.ephemeral_cache_file), exist_ok=True)

        try:
            with open(self.ephemeral_cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
        except IOError as e:
            print(f"Warning: Could not save cluster cache: {e}")

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

        expiry_time = datetime.fromisoformat(cache_data["expiry_time"])
        return datetime.now() < expiry_time
    
    def clear_cluster_cache(self) -> None:
        """
        Clear the cluster cache (for testing or manual cleanup)

        :rtype: None
        """
        if os.path.exists(self.ephemeral_cache_file):
            os.remove(self.ephemeral_cache_file)
            print("Cluster cache cleared")
        else:
            print("No cluster cache to clear")

    def get_cached_cluster_info(self) -> Optional[dict[str, Any]]:
        """
        Get information about the currently cached cluster

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
            "is_valid": self.is_cluster_cache_valid(cache_data)
        }

    def cache_new_cluster(self, cluster_id: str) -> None:
        """
        Cache a new cluster with expiry time

        :param str cluster_id: The ID of the cluster to cache.
        :rtype: None
        """
        expiry_time = datetime.now() + timedelta(hours=self.default_expiry_hours)
        cache_data = {
            "cluster_id": cluster_id,
            "expiry_time": expiry_time.isoformat(),
            "created_at": datetime.now().isoformat()
        }
        self.save_cluster_cache(cache_data)
        print(f"Cached cluster {cluster_id} until {expiry_time}")
