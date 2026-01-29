#!/usr/bin/env python3
"""
Share Capital Record Processor for streaming pipeline
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime, date
import logging
import re

@dataclass
class ShareCapitalRecord:
    """Data class for share capital records using camelCase"""
    reportingDate: str
    capitalCategory: str
    capitalSubCategory: str
    transactionDate: Optional[date]
    transactionType: str
    shareholderName: Optional[str]
    clientType: Optional[str]
    shareholderCountry: str
    numberOfShares: Optional[int]
    sharePriceBookValue: Optional[float]
    currency: str
    orgAmount: Optional[float]
    tzsAmount: Optional[float]
    sectorSnaClassification: str

class ShareCapitalProcessor:
    """Processor for share capital records"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def process_record(self, row, record_type='share_capital'):
        """Process a single share capital record from DB2"""
        try:
            # Parse transaction date properly
            transaction_date = None
            if row[3]:
                try:
                    # Clean up the date string (remove extra spaces)
                    date_str = str(row[3]).strip()
                    # Handle DD/MM/YYYY format
                    if '/' in date_str:
                        parts = date_str.split('/')
                        if len(parts) == 3:
                            day, month, year = parts
                            transaction_date = date(int(year), int(month), int(day))
                    else:
                        # Try to parse as regular date
                        transaction_date = row[3] if isinstance(row[3], date) else None
                except Exception as e:
                    self.logger.warning(f"Could not parse transaction date '{row[3]}': {e}")
                    transaction_date = None
            
            # Map the 14 fields from the SQL query to the dataclass
            record = ShareCapitalRecord(
                reportingDate=str(row[0]).strip() if row[0] else '',
                capitalCategory=str(row[1]).strip() if row[1] else '',
                capitalSubCategory=str(row[2]).strip() if row[2] else '',
                transactionDate=transaction_date,
                transactionType=str(row[4]).strip() if row[4] else '',
                shareholderName=str(row[5]).strip() if row[5] else None,
                clientType=str(row[6]).strip() if row[6] else None,
                shareholderCountry=str(row[7]).strip() if row[7] else '',
                numberOfShares=int(row[8]) if row[8] is not None else None,
                sharePriceBookValue=float(row[9]) if row[9] is not None else None,
                currency=str(row[10]).strip() if row[10] else '',
                orgAmount=float(row[11]) if row[11] is not None else None,
                tzsAmount=float(row[12]) if row[12] is not None else None,
                sectorSnaClassification=str(row[13]).strip() if row[13] else ''
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing share capital record: {e}")
            self.logger.error(f"Row data: {row}")
            raise
    
    def validate_record(self, record):
        """Validate share capital record"""
        try:
            # Basic validation
            if not record.reportingDate:
                self.logger.warning("Missing reporting date")
                return False
            
            if not record.capitalCategory:
                self.logger.warning("Missing capital category")
                return False
            
            if record.orgAmount is not None and record.orgAmount < 0:
                self.logger.warning("Negative original amount")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating share capital record: {e}")
            return False
    
    def insert_to_postgres(self, record, cursor):
        """Insert share capital record to PostgreSQL using camelCase columns"""
        try:
            insert_query = """
            INSERT INTO "shareCapital" (
                "reportingDate", "capitalCategory", "capitalSubCategory", "transactionDate",
                "transactionType", "shareholderName", "clientType", "shareholderCountry",
                "numberOfShares", "sharePriceBookValue", "currency", "orgAmount",
                "tzsAmount", "sectorSnaClassification"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            cursor.execute(insert_query, (
                record.reportingDate,
                record.capitalCategory,
                record.capitalSubCategory,
                record.transactionDate,
                record.transactionType,
                record.shareholderName,
                record.clientType,
                record.shareholderCountry,
                record.numberOfShares,
                record.sharePriceBookValue,
                record.currency,
                record.orgAmount,
                record.tzsAmount,
                record.sectorSnaClassification
            ))
            
        except Exception as e:
            self.logger.error(f"Error inserting share capital record to PostgreSQL: {e}")
            raise