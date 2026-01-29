#!/usr/bin/env python3
"""
Incoming Fund Transfer Record Processor for streaming pipeline
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime, date
import logging

@dataclass
class IncomingFundTransferRecord:
    """Data class for incoming fund transfer records using camelCase"""
    reportingDate: datetime
    transactionId: str
    transactionDate: Optional[date]
    transferChannel: str
    subCategoryTransferChannel: Optional[str]
    recipientName: Optional[str]
    senderAccountNumber: Optional[str]
    recipientIdentificationType: str
    recipientIdentificationNumber: Optional[str]
    recipientCountry: str
    senderName: Optional[str]
    senderBankOrFspCode: Optional[str]
    senderAccountOrWalletNumber: Optional[str]
    serviceCategory: Optional[str]
    serviceSubCategory: Optional[str]
    currency: str
    orgAmount: Optional[float]
    usdAmount: Optional[float]
    tzsAmount: Optional[float]
    purposes: Optional[str]
    senderInstruction: Optional[str]

class IncomingFundTransferProcessor:
    """Processor for incoming fund transfer records"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def process_record(self, row, record_type='incoming_fund_transfer'):
        """Process a single incoming fund transfer record from DB2"""
        try:
            # Map the 21 fields from the SQL query to the dataclass
            record = IncomingFundTransferRecord(
                reportingDate=row[0] if row[0] else datetime.now(),
                transactionId=str(row[1]).strip() if row[1] else '',
                transactionDate=row[2] if row[2] else None,
                transferChannel=str(row[3]).strip() if row[3] else 'EFT',
                subCategoryTransferChannel=str(row[4]).strip() if row[4] else None,
                recipientName=str(row[5]).strip() if row[5] else None,
                senderAccountNumber=str(row[6]).strip() if row[6] else None,
                recipientIdentificationType=str(row[7]).strip() if row[7] else '',
                recipientIdentificationNumber=str(row[8]).strip() if row[8] else None,
                recipientCountry=str(row[9]).strip() if row[9] else 'TANZANIA, UNITED REPUBLIC OF',
                senderName=str(row[10]).strip() if row[10] else None,
                senderBankOrFspCode=str(row[11]).strip() if row[11] else None,
                senderAccountOrWalletNumber=str(row[12]).strip() if row[12] else None,
                serviceCategory=str(row[13]).strip() if row[13] else None,
                serviceSubCategory=str(row[14]).strip() if row[14] else None,
                currency=str(row[15]).strip() if row[15] else '',
                orgAmount=float(row[16]) if row[16] is not None else None,
                usdAmount=float(row[17]) if row[17] is not None else None,
                tzsAmount=float(row[18]) if row[18] is not None else None,
                purposes=str(row[19]).strip() if row[19] else None,
                senderInstruction=str(row[20]).strip() if row[20] else None
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing incoming fund transfer record: {e}")
            self.logger.error(f"Row data: {row}")
            raise
    
    def validate_record(self, record):
        """Validate incoming fund transfer record"""
        try:
            # Basic validation
            if not record.reportingDate:
                self.logger.warning("Missing reporting date")
                return False
            
            if not record.transactionId:
                self.logger.warning("Missing transaction ID")
                return False
            
            if record.orgAmount is not None and record.orgAmount < 0:
                self.logger.warning("Negative original amount")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating incoming fund transfer record: {e}")
            return False
    
    def insert_to_postgres(self, record, cursor):
        """Insert incoming fund transfer record to PostgreSQL using camelCase columns"""
        try:
            insert_query = """
            INSERT INTO "incomingFundTransfer" (
                "reportingDate", "transactionId", "transactionDate", "transferChannel",
                "subCategoryTransferChannel", "recipientName", "senderAccountNumber", 
                "recipientIdentificationType", "recipientIdentificationNumber", "recipientCountry",
                "senderName", "senderBankOrFspCode", "senderAccountOrWalletNumber",
                "serviceCategory", "serviceSubCategory", "currency", "orgAmount", 
                "usdAmount", "tzsAmount", "purposes", "senderInstruction"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            cursor.execute(insert_query, (
                record.reportingDate,
                record.transactionId,
                record.transactionDate,
                record.transferChannel,
                record.subCategoryTransferChannel,
                record.recipientName,
                record.senderAccountNumber,
                record.recipientIdentificationType,
                record.recipientIdentificationNumber,
                record.recipientCountry,
                record.senderName,
                record.senderBankOrFspCode,
                record.senderAccountOrWalletNumber,
                record.serviceCategory,
                record.serviceSubCategory,
                record.currency,
                record.orgAmount,
                record.usdAmount,
                record.tzsAmount,
                record.purposes,
                record.senderInstruction
            ))
            
        except Exception as e:
            self.logger.error(f"Error inserting incoming fund transfer record to PostgreSQL: {e}")
            raise