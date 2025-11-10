"""
ECS Module for DE-Bench

This module provides tools for deploying and managing containerized applications
on AWS ECS (Elastic Container Service).
"""

from .ManifestManager import ECSManifestManager, profile

__all__ = ["ECSManifestManager", "profile"]
