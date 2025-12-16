"""
Data processors for different table types
"""

from .base import BaseProcessor, BaseRecord
from .cash_processor import CashProcessor, CashRecord

__all__ = [
    'BaseProcessor',
    'BaseRecord', 
    'CashProcessor',
    'CashRecord'
]