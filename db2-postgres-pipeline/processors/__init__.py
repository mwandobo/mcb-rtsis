"""
Data processors for different table types
"""

from .base import BaseProcessor, BaseRecord
from .cash_processor import CashProcessor, CashRecord
from .branch_processor import BranchProcessor, BranchRecord
from .atm_processor import AtmProcessor, AtmRecord
from .card_processor import CardProcessor, CardRecord

__all__ = [
    'BaseProcessor',
    'BaseRecord', 
    'CashProcessor',
    'CashRecord',
    'BranchProcessor',
    'BranchRecord',
    'AtmProcessor',
    'AtmRecord',
    'CardProcessor',
    'CardRecord'
]