#!/usr/bin/env python3
"""
ICBM Transaction Record Processor for streaming pipeline
"""

from dataclasses import dataclass
from typing import Optional
from datetime import datetime, date
import logging

@dataclass
class IcbmTransactionRecord:
    """Data class for ICBM transaction records"""
    reportingDate: date
    transactionDate: Optional[date]
    lenderName: Optional[str]
    borrowerName: Optional[str]
    transactionType: str
    tzsAmount: Optional[float]
    tenure: Optional[int]
    interestRate: Optional[float]

class IcbmTransactionProcessor:
    """Processor for ICBM transaction records"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def process_record(self, row, record_type='icbm_transaction'):
        """Process a single ICBM transaction record from DB2"""
        try:
            # Map the 8 fields from the SQL query to the dataclass
            record = IcbmTransactionRecord(
                reportingDate=row[0] if row[0] else date.today(),
                transactionDate=row[1] if row[1] else None,
                lenderName=str(row[2]).strip() if row[2] else None,
                borrowerName=str(row[3]).strip() if row[3] else None,
                transactionType=str(row[4]).strip() if row[4] else 'market',
                tzsAmount=float(row[5]) if row[5] is not None else None,
                tenure=int(row[6]) if row[6] is not None else None,
                interestRate=float(row[7]) if row[7] is not None else None
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing ICBM transaction record: {e}")
            self.logger.error(f"Row data: {row}")
            raise
    
    def validate_record(self, record):
        """Validate ICBM transaction record"""
        try:
            # Basic validation
            if not record.reportingDate:
                self.logger.warning("Missing reporting date")
                return False
            
            if not record.transactionDate:
                self.logger.warning("Missing transaction date")
                return False
            
            if record.tzsAmount is not None and record.tzsAmount < 0:
                self.logger.warning("Negative TZS amount")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating ICBM transaction record: {e}")
            return False
    
    def insert_to_postgres(self, record, cursor):
        """Insert ICBM transaction record to PostgreSQL using camelCase columns"""
        try:
            insert_query = """
            INSERT INTO "icbmTransactions" (
                "reportingDate", "transactionDate", "lenderName", "borrowerName",
                "transactionType", "tzsAmount", tenure, "interestRate"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            cursor.execute(insert_query, (
                record.reportingDate,
                record.transactionDate,
                record.lenderName,
                record.borrowerName,
                record.transactionType,
                record.tzsAmount,
                record.tenure,
                record.interestRate
            ))
            
        except Exception as e:
            self.logger.error(f"Error inserting ICBM transaction record to PostgreSQL: {e}")
            raise