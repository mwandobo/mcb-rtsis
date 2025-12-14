"""
Base processor class for data transformation and loading
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Tuple
from datetime import datetime

@dataclass
class BaseRecord:
    """Base record structure"""
    source_table: str
    timestamp_column_value: str
    original_timestamp: str

class BaseProcessor(ABC):
    """Abstract base class for data processors"""
    
    @abstractmethod
    def process_record(self, raw_data: Tuple, table_name: str) -> BaseRecord:
        """Convert raw DB2 data to structured record"""
        pass
    
    @abstractmethod
    def insert_to_postgres(self, record: BaseRecord, pg_cursor) -> None:
        """Insert record to PostgreSQL"""
        pass
    
    @abstractmethod
    def get_upsert_query(self) -> str:
        """Get the upsert SQL query for this record type"""
        pass
    
    def validate_record(self, record: BaseRecord) -> bool:
        """Validate record before processing"""
        if not record.timestamp_column_value:
            return False
        if not record.source_table:
            return False
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform raw data before creating record"""
        # Override in subclasses for custom transformations
        return {}
    
    def handle_error(self, error: Exception, record: BaseRecord) -> bool:
        """Handle processing errors"""
        # Override in subclasses for custom error handling
        return False