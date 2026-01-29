#!/usr/bin/env python3
"""
Outgoing Fund Transfer Record Processor for streaming pipeline
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime, date
import logging

@dataclass
class OutgoingFundTransferRecord:
    """Data class for outgoing fund transfer records using camelCase"""
    reportingDate: datetime
    transactionId: str
    transactionDate: Optional[date]
    transferChannel: str
    subCategoryTransferChannel: Optional[str]
    senderAccountNumber: Optional[str]
    senderIdentificationType: str
    senderIdentificationNumber: Optional[str]
    recipientName: Optional[str]
    recipientMobileNumber: Optional[str]
    recipientCountry: str
    recipientBankOrFspCode: str
    recipientAccountOrWalletNumber: str
    serviceChannel: str
    serviceCategory: str
    serviceSubCategory: str
    currency: str
    orgAmount: Optional[float]
    usdAmount: Optional[float]
    tzsAmount: Optional[float]
    purposes: Optional[str]
    senderInstruction: str
    transactionPlace: str

class OutgoingFundTransferProcessor:
    """Processor for outgoing fund transfer records"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def process_record(self, row, record_type='outgoing_fund_transfer'):
        """Process a single outgoing fund transfer record from DB2"""
        try:
            # Map the 23 fields from the SQL query to the dataclass
            record = OutgoingFundTransferRecord(
                reportingDate=row[0] if row[0] else datetime.now(),
                transactionId=str(row[1]).strip() if row[1] else '',
                transactionDate=row[2] if row[2] else None,
                transferChannel=str(row[3]).strip() if row[3] else 'EFT',
                subCategoryTransferChannel=str(row[4]).strip() if row[4] else None,
                senderAccountNumber=str(row[5]).strip() if row[5] else None,
                senderIdentificationType=str(row[6]).strip() if row[6] else '',
                senderIdentificationNumber=str(row[7]).strip() if row[7] else None,
                recipientName=str(row[8]).strip() if row[8] else None,
                recipientMobileNumber=str(row[9]).strip() if row[9] else None,
                recipientCountry=str(row[10]).strip() if row[10] else 'TANZANIA, UNITED REPUBLIC OF',
                recipientBankOrFspCode=str(row[11]).strip() if row[11] else 'N/A',
                recipientAccountOrWalletNumber=str(row[12]).strip() if row[12] else 'N/A',
                serviceChannel=str(row[13]).strip() if row[13] else 'Automated Teller Machines',
                serviceCategory=str(row[14]).strip() if row[14] else 'Internet banking',
                serviceSubCategory=str(row[15]).strip() if row[15] else 'Transfer',
                currency=str(row[16]).strip() if row[16] else '',
                orgAmount=float(row[17]) if row[17] is not None else None,
                usdAmount=float(row[18]) if row[18] is not None else None,
                tzsAmount=float(row[19]) if row[19] is not None else None,
                purposes=str(row[20]).strip() if row[20] else None,
                senderInstruction=str(row[21]).strip() if row[21] else 'N/A',
                transactionPlace=str(row[22]).strip() if row[22] else 'TANZANIA, UNITED REPUBLIC OF'
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing outgoing fund transfer record: {e}")
            self.logger.error(f"Row data: {row}")
            raise
    
    def validate_record(self, record):
        """Validate outgoing fund transfer record"""
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
            self.logger.error(f"Error validating outgoing fund transfer record: {e}")
            return False
    
    def insert_to_postgres(self, record, cursor):
        """Insert outgoing fund transfer record to PostgreSQL using camelCase columns"""
        try:
            insert_query = """
            INSERT INTO "outgoingFundTransfer" (
                "reportingDate", "transactionId", "transactionDate", "transferChannel",
                "subCategoryTransferChannel", "senderAccountNumber", "senderIdentificationType", 
                "senderIdentificationNumber", "recipientName", "recipientMobileNumber",
                "recipientCountry", "recipientBankOrFspCode", "recipientAccountOrWalletNumber",
                "serviceChannel", "serviceCategory", "serviceSubCategory", "currency",
                "orgAmount", "usdAmount", "tzsAmount", "purposes", "senderInstruction", "transactionPlace"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            cursor.execute(insert_query, (
                record.reportingDate,
                record.transactionId,
                record.transactionDate,
                record.transferChannel,
                record.subCategoryTransferChannel,
                record.senderAccountNumber,
                record.senderIdentificationType,
                record.senderIdentificationNumber,
                record.recipientName,
                record.recipientMobileNumber,
                record.recipientCountry,
                record.recipientBankOrFspCode,
                record.recipientAccountOrWalletNumber,
                record.serviceChannel,
                record.serviceCategory,
                record.serviceSubCategory,
                record.currency,
                record.orgAmount,
                record.usdAmount,
                record.tzsAmount,
                record.purposes,
                record.senderInstruction,
                record.transactionPlace
            ))
            
        except Exception as e:
            self.logger.error(f"Error inserting outgoing fund transfer record to PostgreSQL: {e}")
            raise