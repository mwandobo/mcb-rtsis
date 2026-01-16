"""
Card information processor - Based on card_information.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class CardRecord(BaseRecord):
    """Card information record structure - Based on card_information.sql"""
    reporting_date: str
    bank_code: str
    card_number: str
    bin_number: str
    customer_identification_number: str
    card_type: str
    card_type_sub_category: Optional[str]
    card_issue_date: str
    card_issuer: str
    card_issuer_category: str
    card_issuer_country: str
    card_holder_name: str
    card_status: str
    card_scheme: str
    acquiring_partner: str
    card_expire_date: str
    retry_count: int = 0

class CardProcessor(BaseProcessor):
    """Processor for card information data - Based on card_information.sql"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> CardRecord:
        """Convert raw DB2 data to CardRecord - 15 fields after removing reportingDate"""
        # raw_data structure (after removing reportingDate): bankCode, cardNumber, binNumber, customerIdentificationNumber, 
        # cardType, cardTypeSubCategory, cardIssueDate, cardIssuer, cardIssuerCategory, cardIssuerCountry, 
        # cardHolderName, cardStatus, cardScheme, acquiringPartner, cardExpireDate
        return CardRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[6]),  # cardIssueDate for tracking (7th field, index 6)
            reporting_date=datetime.now().isoformat(),  # Use current timestamp for reporting date
            bank_code=str(raw_data[0]),
            card_number=str(raw_data[1]),
            bin_number=str(raw_data[2]),
            customer_identification_number=str(raw_data[3]),
            card_type=str(raw_data[4]),
            card_type_sub_category=str(raw_data[5]) if raw_data[5] and str(raw_data[5]).strip() not in ('None', 'NULL', '') else None,
            card_issue_date=str(raw_data[6]),
            card_issuer=str(raw_data[7]),
            card_issuer_category=str(raw_data[8]),
            card_issuer_country=str(raw_data[9]),
            card_holder_name=str(raw_data[10]),
            card_status=str(raw_data[11]),
            card_scheme=str(raw_data[12]),
            acquiring_partner=str(raw_data[13]),
            card_expire_date=str(raw_data[14]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: CardRecord, pg_cursor) -> None:
        """Insert card record to PostgreSQL"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.bank_code,
            record.card_number,
            record.bin_number,
            record.customer_identification_number,
            record.card_type,
            record.card_type_sub_category,
            record.card_issue_date,
            record.card_issuer,
            record.card_issuer_category,
            record.card_issuer_country,
            record.card_holder_name,
            record.card_status,
            record.card_scheme,
            record.acquiring_partner,
            record.card_expire_date
        ))
    
    def get_upsert_query(self) -> str:
        """Get insert query for card information"""
        return """
        INSERT INTO "cardInformation" (
            "reportingDate", "bankCode", "cardNumber", "binNumber", "customerIdentificationNumber",
            "cardType", "cardTypeSubCategory", "cardIssueDate", "cardIssuer", "cardIssuerCategory",
            "cardIssuerCountry", "cardHolderName", "cardStatus", "cardScheme", "acquiringPartner",
            "cardExpireDate"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def validate_record(self, record: CardRecord) -> bool:
        """Validate card record"""
        if not super().validate_record(record):
            return False
        
        # Card-specific validations
        if not record.card_number:
            return False
        if not record.customer_identification_number:
            return False
        if not record.card_type:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform card data"""
        # Add any card-specific transformations here
        return {
            'card_number_masked': str(raw_data[1])[:4] + '****' + str(raw_data[1])[-4:] if len(str(raw_data[1])) >= 8 else str(raw_data[1]),
            'card_type_normalized': str(raw_data[4]).strip().upper(),
            'card_status_normalized': str(raw_data[11]).strip().lower()
        }