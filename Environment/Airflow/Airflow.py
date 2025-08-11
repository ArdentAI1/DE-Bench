"""
This module re-exports the Airflow_Local class from Fixtures.Airflow.Airflow
for backward compatibility with older test imports.
"""

from Fixtures.Airflow.Airflow import Airflow_Local

__all__ = ['Airflow_Local'] 