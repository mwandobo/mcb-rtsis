"""
Card transaction processor - Based on card_transaction.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class CardTransactionRecord(BaseRecord):
    """Card transaction record structure - Based on card_transaction.sql"""
    reporting_date: str
    card_number: str
    bin_number: str
    transacting_bank_name: str
    transaction_id: str
    transaction_date: str
    transaction_nature: str
    atm_code: Optional[str]
    pos_number: Optional[str]
    transaction_description: str
    beneficiary_name: str
    beneficiary_trade_name: Optional[str]
    beneficiary_country: str
    transaction_place: str
    qty_items_purchased: Optional[str]
    unit_price: Optional[str]
    org_facilitator_commission_amount: Optional[str]
    usd_facilitator_commission_amount: Optional[str]
    tzs_facilitator_commission_amount: Optional[str]
    currency: str
    org_transaction_amount: str
    usd_transaction_amount: str
    tzs_transaction_amount: str
    retry_count: int = 0

class CardTransactionProcessor(BaseProcessor):
    """Processor for card transaction data - Based on card_transaction.sql"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> CardTransactionRecord:
        """Convert raw DB2 data to CardTransactionRecord"""
        # raw_data structure: reportingDate, cardNumber, binNumber, transactingBankName, transactionId,
        # transactionDate, transactionNature, atmCode, posNumber, transactionDescription, beneficiaryName,
        # beneficiaryTradeName, beneficiaryCountry, transactionPlace, qtyItemsPurchased, unitPrice,
        # orgFacilitatorCommissionAmount, usdFacilitatorCommissionAmount, tzsFacilitatorCommissionAmount,
        # currency, orgTransactionAmount, usdTransactionAmount, tzsTransactionAmount
        return CardTransactionRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[5]),  # transactionDate for tracking
            reporting_date=str(raw_data[0]),
            card_number=str(raw_data[1]),
            bin_number=str(raw_data[2]),
            transacting_bank_name=str(raw_data[3]),
            transaction_id=str(raw_data[4]).strip(),  # Strip whitespace from transaction ID
            transaction_date=str(raw_data[5]),
            transaction_nature=str(raw_data[6]),
            atm_code=str(raw_data[7]).strip() if raw_data[7] else None,
            pos_number=str(raw_data[8]).strip() if raw_data[8] else None,
            transaction_description=str(raw_data[9]),
            beneficiary_name=str(raw_data[10]),
            beneficiary_trade_name=str(raw_data[11]).strip() if raw_data[11] else None,
            beneficiary_country=str(raw_data[12]),
            transaction_place=str(raw_data[13]),
            qty_items_purchased=str(raw_data[14]).strip() if raw_data[14] else None,
            unit_price=str(raw_data[15]).strip() if raw_data[15] else None,
            org_facilitator_commission_amount=str(raw_data[16]).strip() if raw_data[16] else None,
            usd_facilitator_commission_amount=str(raw_data[17]).strip() if raw_data[17] else None,
            tzs_facilitator_commission_amount=str(raw_data[18]).strip() if raw_data[18] else None,
            currency=str(raw_data[19]),
            org_transaction_amount=str(raw_data[20]),
            usd_transaction_amount=str(raw_data[21]),
            tzs_transaction_amount=str(raw_data[22]),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: CardTransactionRecord, pg_cursor) -> None:
        """Insert card transaction record to PostgreSQL"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.card_number,
            record.bin_number,
            record.transacting_bank_name,
            record.transaction_id,
            record.transaction_date,
            record.transaction_nature,
            record.atm_code,
            record.pos_number,
            record.transaction_description,
            record.beneficiary_name,
            record.beneficiary_trade_name,
            record.beneficiary_country,
            record.transaction_place,
            record.qty_items_purchased,
            record.unit_price,
            record.org_facilitator_commission_amount,
            record.usd_facilitator_commission_amount,
            record.tzs_facilitator_commission_amount,
            record.currency,
            record.org_transaction_amount,
            record.usd_transaction_amount,
            record.tzs_transaction_amount
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for card transaction with duplicate handling"""
        return """
        INSERT INTO "cardTransaction" (
            "reportingDate", "cardNumber", "binNumber", "transactingBankName", "transactionId",
            "transactionDate", "transactionNature", "atmCode", "posNumber", "transactionDescription",
            "beneficiaryName", "beneficiaryTradeName", "beneficiaryCountry", "transactionPlace",
            "qtyItemsPurchased", "unitPrice", "orgFacilitatorCommissionAmount", "usdFacilitatorCommissionAmount",
            "tzsFacilitatorCommissionAmount", "currency", "orgTransactionAmount", "usdTransactionAmount",
            "tzsTransactionAmount"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("transactionId") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "cardNumber" = EXCLUDED."cardNumber",
            "binNumber" = EXCLUDED."binNumber",
            "transactingBankName" = EXCLUDED."transactingBankName",
            "transactionDate" = EXCLUDED."transactionDate",
            "transactionNature" = EXCLUDED."transactionNature",
            "atmCode" = EXCLUDED."atmCode",
            "posNumber" = EXCLUDED."posNumber",
            "transactionDescription" = EXCLUDED."transactionDescription",
            "beneficiaryName" = EXCLUDED."beneficiaryName",
            "beneficiaryTradeName" = EXCLUDED."beneficiaryTradeName",
            "beneficiaryCountry" = EXCLUDED."beneficiaryCountry",
            "transactionPlace" = EXCLUDED."transactionPlace",
            "qtyItemsPurchased" = EXCLUDED."qtyItemsPurchased",
            "unitPrice" = EXCLUDED."unitPrice",
            "orgFacilitatorCommissionAmount" = EXCLUDED."orgFacilitatorCommissionAmount",
            "usdFacilitatorCommissionAmount" = EXCLUDED."usdFacilitatorCommissionAmount",
            "tzsFacilitatorCommissionAmount" = EXCLUDED."tzsFacilitatorCommissionAmount",
            "currency" = EXCLUDED."currency",
            "orgTransactionAmount" = EXCLUDED."orgTransactionAmount",
            "usdTransactionAmount" = EXCLUDED."usdTransactionAmount",
            "tzsTransactionAmount" = EXCLUDED."tzsTransactionAmount"
        """
    
    def validate_record(self, record: CardTransactionRecord) -> bool:
        """Validate card transaction record"""
        if not super().validate_record(record):
            return False
        
        # Card transaction-specific validations
        if not record.card_number:
            return False
        if not record.transaction_id:
            return False
        if not record.transaction_date:
            return False
        if not record.org_transaction_amount:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform card transaction data"""
        # Add any card transaction-specific transformations here
        return {
            'card_number_masked': str(raw_data[1])[:4] + '****' + str(raw_data[1])[-4:] if len(str(raw_data[1])) >= 8 else str(raw_data[1]),
            'transaction_nature_normalized': str(raw_data[6]).strip().upper(),
            'currency_normalized': str(raw_data[19]).strip().upper()
        }