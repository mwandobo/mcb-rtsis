"""
POS Information processor with camelCase naming
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class PosInformationRecord(BaseRecord):
    """POS Information record structure with camelCase fields"""
    reportingDate: str
    posBranchCode: int
    posNumber: str
    qrFsrCode: str
    posHolderCategory: str
    posHolderName: str
    posHolderNin: Optional[str]
    posHolderTin: str
    postalCode: Optional[str]
    region: str
    district: str
    ward: str
    street: str
    houseNumber: str
    gpsCoordinates: Optional[str]
    linkedAccount: str
    issueDate: str
    returnDate: Optional[str]
    retry_count: int = 0

class PosInformationProcessor(BaseProcessor):
    """Processor for POS information data with camelCase naming"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> PosInformationRecord:
        """Convert raw DB2 data to PosInformationRecord"""
        return PosInformationRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]) if raw_data[0] else '',  # reportingDate
            reportingDate=str(raw_data[0]) if raw_data[0] else '',
            posBranchCode=int(raw_data[1]) if raw_data[1] is not None else 0,
            posNumber=str(raw_data[2]) if raw_data[2] else '',
            qrFsrCode=str(raw_data[3]) if raw_data[3] else '',
            posHolderCategory=str(raw_data[4]) if raw_data[4] else '',
            posHolderName=str(raw_data[5]) if raw_data[5] else '',
            posHolderNin=str(raw_data[6]) if raw_data[6] else None,
            posHolderTin=str(raw_data[7]) if raw_data[7] else '',
            postalCode=str(raw_data[8]) if raw_data[8] else None,
            region=str(raw_data[9]) if raw_data[9] else '',
            district=str(raw_data[10]) if raw_data[10] else '',
            ward=str(raw_data[11]) if raw_data[11] else '',
            street=str(raw_data[12]) if raw_data[12] else '',
            houseNumber=str(raw_data[13]) if raw_data[13] else '',
            gpsCoordinates=str(raw_data[14]) if raw_data[14] else None,
            linkedAccount=str(raw_data[15]) if raw_data[15] else '',
            issueDate=str(raw_data[16]) if raw_data[16] else '',
            returnDate=str(raw_data[17]) if raw_data[17] else None,
            original_timestamp=datetime.now().isoformat()
        )
    
    def create_record_from_dict(self, record_data: dict) -> PosInformationRecord:
        """Create PosInformationRecord from dictionary (for RabbitMQ consumption)"""
        return PosInformationRecord(
            source_table='posInformation',
            timestamp_column_value=record_data.get('reportingDate', ''),
            reportingDate=record_data.get('reportingDate', ''),
            posBranchCode=int(record_data.get('posBranchCode', 0)),
            posNumber=record_data.get('posNumber', ''),
            qrFsrCode=record_data.get('qrFsrCode', ''),
            posHolderCategory=record_data.get('posHolderCategory', ''),
            posHolderName=record_data.get('posHolderName', ''),
            posHolderNin=record_data.get('posHolderNin'),
            posHolderTin=record_data.get('posHolderTin', ''),
            postalCode=record_data.get('postalCode'),
            region=record_data.get('region', ''),
            district=record_data.get('district', ''),
            ward=record_data.get('ward', ''),
            street=record_data.get('street', ''),
            houseNumber=record_data.get('houseNumber', ''),
            gpsCoordinates=record_data.get('gpsCoordinates'),
            linkedAccount=record_data.get('linkedAccount', ''),
            issueDate=record_data.get('issueDate', ''),
            returnDate=record_data.get('returnDate'),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: PosInformationRecord, pg_cursor) -> None:
        """Insert POS Information record to PostgreSQL with camelCase table and fields"""
        query = """
        INSERT INTO "posInformation" (
            "reportingDate", "posBranchCode", "posNumber", "qrFsrCode", "posHolderCategory",
            "posHolderName", "posHolderNin", "posHolderTin", "postalCode", "region",
            "district", "ward", "street", "houseNumber", "gpsCoordinates",
            "linkedAccount", "issueDate", "returnDate"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("posNumber") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "posBranchCode" = EXCLUDED."posBranchCode",
            "qrFsrCode" = EXCLUDED."qrFsrCode",
            "posHolderCategory" = EXCLUDED."posHolderCategory",
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
        
        pg_cursor.execute(query, (
            record.reportingDate,
            record.posBranchCode,
            record.posNumber,
            record.qrFsrCode,
            record.posHolderCategory,
            record.posHolderName,
            record.posHolderNin,
            record.posHolderTin,
            record.postalCode,
            record.region,
            record.district,
            record.ward,
            record.street,
            record.houseNumber,
            record.gpsCoordinates,
            record.linkedAccount,
            record.issueDate,
            record.returnDate
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for posInformation"""
        return """
        INSERT INTO "posInformation" (
            "reportingDate", "posBranchCode", "posNumber", "qrFsrCode", "posHolderCategory",
            "posHolderName", "posHolderNin", "posHolderTin", "postalCode", "region",
            "district", "ward", "street", "houseNumber", "gpsCoordinates",
            "linkedAccount", "issueDate", "returnDate"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def validate_record(self, record: PosInformationRecord) -> bool:
        """Validate POS Information record"""
        if not super().validate_record(record):
            return False
        
        # Basic validations
        if not record.posNumber:
            return False
        if not record.qrFsrCode:
            return False
        if not record.posHolderName:
            return False
            
        return True