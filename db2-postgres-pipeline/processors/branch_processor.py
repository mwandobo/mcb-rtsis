"""
Branch information processor
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class BranchRecord(BaseRecord):
    """Branch information record structure"""
    reporting_date: str
    branch_name: str
    tax_identification_number: str
    business_license: str
    branch_code: str
    qr_fsr_code: str
    region: str
    district: str
    ward: str
    street: Optional[str]
    house_number: Optional[str]
    postal_code: Optional[str]
    gps_coordinates: str
    banking_services: str
    mobile_money_services: str
    registration_date: str
    branch_status: str
    closure_date: Optional[str]
    contact_person: str
    telephone_number: str
    alt_telephone_number: Optional[str]
    branch_category: str
    last_modified: str
    retry_count: int = 0

class BranchProcessor(BaseProcessor):
    """Processor for branch information data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> BranchRecord:
        """Convert raw DB2 data to BranchRecord"""
        # raw_data structure from branch1.sql:
        # 0: reportingDate, 1: branchName, 2: taxIdentificationNumber, 3: businessLicense,
        # 4: branchCode, 5: qrFsrCode, 6: region, 7: district, 8: ward, 9: street,
        # 10: houseNumber, 11: postalCode, 12: gpsCoordinates, 13: bankingServices,
        # 14: mobileMoneyServices, 15: registrationDate, 16: branchStatus, 17: closureDate,
        # 18: contactPerson, 19: telephoneNumber, 20: altTelephoneNumber, 21: branchCategory,
        # 22: lastModified
        
        return BranchRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[22]),  # lastModified for tracking
            reporting_date=self._convert_db2_timestamp(str(raw_data[0])),
            branch_name=str(raw_data[1]),
            tax_identification_number=str(raw_data[2]),
            business_license=str(raw_data[3]),
            branch_code=str(raw_data[4]),
            qr_fsr_code=str(raw_data[5]),
            region=str(raw_data[6]).strip() if raw_data[6] and str(raw_data[6]).strip() else 'Unknown Region',
            district=str(raw_data[7]).strip() if raw_data[7] and str(raw_data[7]).strip() else 'Unknown District',
            ward=str(raw_data[8]),
            street=str(raw_data[9]) if raw_data[9] else None,
            house_number=str(raw_data[10]) if raw_data[10] else None,
            postal_code=str(raw_data[11]) if raw_data[11] else None,
            gps_coordinates=str(raw_data[12]),
            banking_services=str(raw_data[13]),
            mobile_money_services=str(raw_data[14]),
            registration_date=self._convert_db2_date(str(raw_data[15])),
            branch_status=str(raw_data[16]),
            closure_date=self._convert_db2_date(str(raw_data[17])) if raw_data[17] else None,
            contact_person=str(raw_data[18]),
            telephone_number=str(raw_data[19]),
            alt_telephone_number=str(raw_data[20]) if raw_data[20] else None,
            branch_category=str(raw_data[21]),
            last_modified=str(raw_data[22]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def _convert_db2_timestamp(self, db2_timestamp: str) -> str:
        """Convert DB2 DDMMYYYYHHMM format to PostgreSQL timestamp format"""
        try:
            if not db2_timestamp or db2_timestamp == 'None':
                return None
            
            # Parse DDMMYYYYHHMM format
            if len(db2_timestamp) == 12:
                day = db2_timestamp[0:2]
                month = db2_timestamp[2:4]
                year = db2_timestamp[4:8]
                hour = db2_timestamp[8:10]
                minute = db2_timestamp[10:12]
                
                # Convert to PostgreSQL timestamp format: YYYY-MM-DD HH:MM:SS
                return f"{year}-{month}-{day} {hour}:{minute}:00"
            else:
                # If format is unexpected, return current timestamp
                return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            # If conversion fails, return current timestamp
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def _convert_db2_date(self, db2_timestamp: str) -> str:
        """Convert DB2 DDMMYYYYHHMM format to PostgreSQL date format"""
        try:
            if not db2_timestamp or db2_timestamp == 'None':
                return None
            
            # Parse DDMMYYYYHHMM format
            if len(db2_timestamp) == 12:
                day = db2_timestamp[0:2]
                month = db2_timestamp[2:4]
                year = db2_timestamp[4:8]
                
                # Convert to PostgreSQL date format: YYYY-MM-DD
                return f"{year}-{month}-{day}"
            else:
                # If format is unexpected, return current date
                return datetime.now().strftime('%Y-%m-%d')
        except Exception:
            # If conversion fails, return current date
            return datetime.now().strftime('%Y-%m-%d')
    
    def insert_to_postgres(self, record: BranchRecord, pg_cursor) -> None:
        """Insert branch record to PostgreSQL"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.branch_name,
            record.tax_identification_number,
            record.business_license,
            record.branch_code,
            record.qr_fsr_code,
            record.region,
            record.district,
            record.ward,
            record.street,
            record.house_number,
            record.postal_code,
            record.gps_coordinates,
            record.banking_services,
            record.mobile_money_services,
            record.registration_date,
            record.branch_status,
            record.closure_date,
            record.contact_person,
            record.telephone_number,
            record.alt_telephone_number,
            record.branch_category,
            record.last_modified
        ))
    
    def get_upsert_query(self) -> str:
        """Get insert query for branch information with camelCase"""
        return """
        INSERT INTO "branch" (
            "reportingDate", "branchName", "taxIdentificationNumber", "businessLicense",
            "branchCode", "qrFsrCode", "region", "district", "ward", "street",
            "houseNumber", "postalCode", "gpsCoordinates", "bankingServices",
            "mobileMoneyServices", "registrationDate", "branchStatus", "closureDate",
            "contactPerson", "telephoneNumber", "altTelephoneNumber", "branchCategory", "lastModified"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("branchCode") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "branchName" = EXCLUDED."branchName",
            "taxIdentificationNumber" = EXCLUDED."taxIdentificationNumber",
            "businessLicense" = EXCLUDED."businessLicense",
            "qrFsrCode" = EXCLUDED."qrFsrCode",
            "region" = EXCLUDED."region",
            "district" = EXCLUDED."district",
            "ward" = EXCLUDED."ward",
            "street" = EXCLUDED."street",
            "houseNumber" = EXCLUDED."houseNumber",
            "postalCode" = EXCLUDED."postalCode",
            "gpsCoordinates" = EXCLUDED."gpsCoordinates",
            "bankingServices" = EXCLUDED."bankingServices",
            "mobileMoneyServices" = EXCLUDED."mobileMoneyServices",
            "registrationDate" = EXCLUDED."registrationDate",
            "branchStatus" = EXCLUDED."branchStatus",
            "closureDate" = EXCLUDED."closureDate",
            "contactPerson" = EXCLUDED."contactPerson",
            "telephoneNumber" = EXCLUDED."telephoneNumber",
            "altTelephoneNumber" = EXCLUDED."altTelephoneNumber",
            "branchCategory" = EXCLUDED."branchCategory",
            "lastModified" = EXCLUDED."lastModified"
        """
    
    def validate_record(self, record: BranchRecord) -> bool:
        """Validate branch record"""
        if not super().validate_record(record):
            return False
        
        # Branch-specific validations - only check essential fields
        if not record.branch_code or record.branch_code.strip() == '':
            return False
        if not record.branch_name or record.branch_name.strip() == '':
            return False
        if not record.branch_status or record.branch_status.strip() == '':
            return False
        # Region can be empty - we'll use 'Unknown Region' as default
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform branch data"""
        # Add any branch-specific transformations here
        return {
            'branch_code_normalized': str(raw_data[4]).strip().upper(),
            'branch_name_normalized': str(raw_data[1]).strip(),
            'status_validated': str(raw_data[16]).strip() if raw_data[16] else 'Unknown'
        }
