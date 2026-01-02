"""
ATM information processor - Updated to match existing ATM SQL query
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class AtmRecord(BaseRecord):
    """ATM information record structure - Updated for BANKEMPLOYEE-based query with atmCategory"""
    reporting_date: str
    atm_name: str
    branch_code: str
    atm_code: str
    till_number: str
    mobile_money_services: str
    qr_fsr_code: str
    postal_code: Optional[str]
    region: str
    district: str
    ward: str
    street: str
    house_number: Optional[str]
    gps_coordinates: str
    linked_account: str
    opening_date: str
    atm_status: str
    closure_date: Optional[str]
    atm_category: str
    atm_channel: str
    retry_count: int = 0

class AtmProcessor(BaseProcessor):
    """Processor for ATM information data - Updated for BANKEMPLOYEE-based query"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> AtmRecord:
        """Convert raw DB2 data to AtmRecord - 19 fields after removing reportingDate"""
        # raw_data structure (after removing reportingDate): atmName, branchCode, atmCode, tillNumber, 
        # mobileMoneyServices, qrFsrCode, postalCode, region, district, ward, street, 
        # houseNumber, gpsCoordinates, linkedAccount, openingDate, atmStatus, closureDate, atmCategory, atmChannel
        return AtmRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[14]),  # openingDate for tracking (15th field, index 14)
            reporting_date="",  # Not available after removing from query
            atm_name=str(raw_data[0]),
            branch_code=str(raw_data[1]),
            atm_code=str(raw_data[2]),
            till_number=str(raw_data[3]),
            mobile_money_services=str(raw_data[4]),
            qr_fsr_code=str(raw_data[5]),
            postal_code=str(raw_data[6]) if raw_data[6] else None,
            region=str(raw_data[7]),
            district=str(raw_data[8]),
            ward=str(raw_data[9]),
            street=str(raw_data[10]),
            house_number=str(raw_data[11]) if raw_data[11] else None,
            gps_coordinates=str(raw_data[12]),
            linked_account=str(raw_data[13]),
            opening_date=str(raw_data[14]),
            atm_status=str(raw_data[15]),
            closure_date=str(raw_data[16]) if raw_data[16] else None,
            atm_category=str(raw_data[17]),
            atm_channel=str(raw_data[18]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: AtmRecord, pg_cursor) -> None:
        """Insert ATM record to PostgreSQL - Updated to include atmCategory"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.atm_name,
            record.branch_code,
            record.atm_code,
            record.till_number,
            record.mobile_money_services,
            record.qr_fsr_code,
            record.postal_code,
            record.region,
            record.district,
            record.ward,
            record.street,
            record.house_number,
            record.gps_coordinates,
            record.linked_account,
            record.opening_date,
            record.atm_status,
            record.closure_date,
            record.atm_category,
            record.atm_channel
        ))
    
    def get_upsert_query(self) -> str:
        """Get insert query for ATM information - Updated to include atmCategory"""
        return """
        INSERT INTO "atmInformation" (
            "reportingDate", "atmName", "branchCode", "atmCode", "tillNumber",
            "mobileMoneyServices", "qrFsrCode", "postalCode", "region", "district",
            "ward", "street", "houseNumber", "gpsCoordinates", "linkedAccount",
            "openingDate", "atmStatus", "closureDate", "atmCategory", "atmChannel"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def validate_record(self, record: AtmRecord) -> bool:
        """Validate ATM record"""
        if not super().validate_record(record):
            return False
        
        # ATM-specific validations
        if not record.atm_code:
            return False
        if not record.branch_code:
            return False
        if not record.atm_name:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform ATM data"""
        # Add any ATM-specific transformations here
        return {
            'atm_code_normalized': str(raw_data[3]).strip().upper(),
            'branch_code_validated': str(raw_data[2]).strip(),
            'status_normalized': str(raw_data[16]).strip().lower()
        }