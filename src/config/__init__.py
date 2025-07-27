"""
Configuration Package

Contains all configuration settings and schemas for the ETL pipeline.
"""

from .config import WeatherConfig, config

__all__ = ['WeatherConfig', 'config'] 