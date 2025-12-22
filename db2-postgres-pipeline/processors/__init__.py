"""
Data processors for different table types
"""

from .base import BaseProcessor, BaseRecord
from .cash_processor import CashProcessor, CashRecord
from .branch_processor import BranchProcessor, BranchRecord

__all__ = [
    'BaseProcessor',
    'BaseRecord', 
    'CashProcessor',
    'CashRecord',
    'BranchProcessor',
    'BranchRecord'
]