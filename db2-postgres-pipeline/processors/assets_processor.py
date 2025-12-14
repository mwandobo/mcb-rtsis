"""
Assets owned processor
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class AssetsRecord(BaseRecord):
    """Assets owned record structure"""
    reporting_date: str
    acquisition_date: str
    currency: str
    asset_category: str
    asset_type: str
    org_cost_value: float
    usd_cost_value: Optional[float]
    tzs_cost_value: float
    allowance_probable_loss: float = 0.0
    bot_provision: float = 0.0
    retry_count: int = 0

class AssetsProcessor(BaseProcessor):
    """Processor for assets owned data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> AssetsRecord:
        """Convert raw DB2 data to AssetsRecord"""
        return AssetsRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]),  # reportingDate
            reporting_date=str(raw_data[0]),
            acquisition_date=str(raw_data[1]),
            currency=str(raw_data[2]),
            asset_category=str(raw_data[3]),
            asset_type=str(raw_data[4]),
            org_cost_value=float(raw_data[5]),
            usd_cost_value=float(raw_data[6]) if raw_data[6] else None,
            tzs_cost_value=float(raw_data[7]),
            allowance_probable_loss=float(raw_data[8]),
            bot_provision=float(raw_data[9]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: AssetsRecord, pg_cursor) -> None:
        """Insert assets record to PostgreSQL"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.acquisition_date,
            record.currency,
            record.asset_category,
            record.asset_type,
            record.org_cost_value,
            record.usd_cost_value,
            record.tzs_cost_value,
            record.allowance_probable_loss,
            record.bot_provision
        ))
    
    def get_upsert_query(self) -> str:
        """Get insert query for assets owned"""
        return """
        INSERT INTO asset_owned (
            "reportingDate", "acquisitionDate", currency, "assetCategory", "assetType",
            "orgCostValue", "usdCostValue", "tzsCostValue", "allowanceProbableLoss", "botProvision"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def validate_record(self, record: AssetsRecord) -> bool:
        """Validate assets record"""
        if not super().validate_record(record):
            return False
        
        # Assets-specific validations
        if not record.asset_type:
            return False
        if record.org_cost_value is None:
            return False
        if not record.currency:
            return False
        if not record.asset_category:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform assets data"""
        # Add any assets-specific transformations here
        return {
            'currency_normalized': str(raw_data[2]).strip().upper(),
            'cost_value_validated': max(0, float(raw_data[5])) if raw_data[5] else 0
        }