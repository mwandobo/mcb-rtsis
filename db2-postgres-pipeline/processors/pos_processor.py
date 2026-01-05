"""
POS information processor - Based on pos-v1.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class POSRecord(BaseRecord):
    """POS information record structure - Based on pos-v1.sql"""
    reporting_date: str
    pos_branch_code: str
    pos_number: str
    qr_fsr_code: str
    pos_holder_name: str
    pos_holder_nin: Optional[str]
    pos_holder_tin: str
    postal_code: Optional[str]
    region: str
    district: str
    ward: Optional[str]
    street: Optional[str]
    house_number: Optional[str]
    gps_coordinates: Optional[str]
    linked_account: str
    issue_date: str
    return_date: Optional[str]
    retry_count: int = 0

class POSProcessor(BaseProcessor):
    """Processor for POS information data - Based on pos-v1.sql"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> POSRecord:
        """Convert raw DB2 data to POSRecord - Updated for pos-v1.sql"""
        # raw_data structure from pos-v1.sql (17 fields):
        # 0: reportingDate, 1: posBranchCode, 2: posNumber, 3: qrFsrCode, 4: posHolderName, 
        # 5: posHolderNin, 6: posHolderTin, 7: postalCode, 8: region, 9: district, 
        # 10: ward, 11: street, 12: houseNumber, 13: gpsCoordinates, 14: linkedAccount, 
        # 15: issueDate, 16: returnDate
        return POSRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[15]),  # issueDate for tracking
            reporting_date=str(raw_data[0]),
            pos_branch_code=str(raw_data[1]),
            pos_number=str(raw_data[2]),
            qr_fsr_code=str(raw_data[3]),
            pos_holder_name=str(raw_data[4]),
            pos_holder_nin=str(raw_data[5]) if raw_data[5] else None,
            pos_holder_tin=str(raw_data[6]),
            postal_code=str(raw_data[7]) if raw_data[7] else None,
            region=str(raw_data[8]),
            district=str(raw_data[9]),
            ward=str(raw_data[10]) if raw_data[10] else None,
            street=str(raw_data[11]) if raw_data[11] else None,
            house_number=str(raw_data[12]) if raw_data[12] else None,
            gps_coordinates=str(raw_data[13]) if raw_data[13] else None,
            linked_account=str(raw_data[14]),
            issue_date=str(raw_data[15]),
            return_date=str(raw_data[16]) if raw_data[16] else None,
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: POSRecord, pg_cursor) -> None:
        """Insert POS record to PostgreSQL"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.pos_branch_code,
            record.pos_number,
            record.qr_fsr_code,
            record.pos_holder_name,
            record.pos_holder_nin,
            record.pos_holder_tin,
            record.postal_code,
            record.region,
            record.district,
            record.ward,
            record.street,
            record.house_number,
            record.gps_coordinates,
            record.linked_account,
            record.issue_date,
            record.return_date
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for POS information with duplicate handling"""
        return """
        INSERT INTO "posInformation" (
            "reportingDate", "posBranchCode", "posNumber", "qrFsrCode", "posHolderName",
            "posHolderNin", "posHolderTin", "postalCode", "region", "district",
            "ward", "street", "houseNumber", "gpsCoordinates", "linkedAccount",
            "issueDate", "returnDate"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("posNumber") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "posBranchCode" = EXCLUDED."posBranchCode",
            "qrFsrCode" = EXCLUDED."qrFsrCode",
            "posHolderName" = EXCLUDED."posHolderName",
            "posHolderNin" = EXCLUDED."posHolderNin",
            "posHolderTin" = EXCLUDED."posHolderTin",
            "postalCode" = EXCLUDED."postalCode",
            "region" = EXCLUDED."region",
            "district" = EXCLUDED."district",
            "ward" = EXCLUDED."ward",
            "street" = EXCLUDED."street",
            "houseNumber" = EXCLUDED."houseNumber",
            "gpsCoordinates" = EXCLUDED."gpsCoordinates",
            "linkedAccount" = EXCLUDED."linkedAccount",
            "issueDate" = EXCLUDED."issueDate",
            "returnDate" = EXCLUDED."returnDate"
        """
    
    def validate_record(self, record: POSRecord) -> bool:
        """Validate POS record"""
        if not super().validate_record(record):
            return False
        
        # POS-specific validations
        if not record.pos_number:
            return False
        if not record.pos_holder_name:
            return False
        if not record.region:
            return False
        if not record.district:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform POS data"""
        # Add any POS-specific transformations here
        return {
            'pos_number_str': str(raw_data[2]),
            'region_normalized': str(raw_data[8]).strip().upper(),
            'district_normalized': str(raw_data[9]).strip().upper()
        }