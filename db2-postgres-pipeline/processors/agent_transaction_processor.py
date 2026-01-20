"""
Agent Transaction processor with camelCase naming
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class AgentTransactionRecord(BaseRecord):
    """Agent Transaction record structure with camelCase fields"""
    reportingDate: str
    agentId: str
    agentStatus: str
    transactionDate: str
    transactionId: str
    transactionType: str
    serviceChannel: str
    tillNumber: Optional[str]
    currency: str
    tzsAmount: float
    retry_count: int = 0

class AgentTransactionProcessor(BaseProcessor):
    """Processor for agent transaction data with camelCase naming"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> AgentTransactionRecord:
        """Convert raw DB2 data to AgentTransactionRecord"""
        return AgentTransactionRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[3]) if raw_data[3] else '',  # transactionDate
            reportingDate=str(raw_data[0]) if raw_data[0] else '',
            agentId=str(raw_data[1]) if raw_data[1] else '',
            agentStatus=str(raw_data[2]) if raw_data[2] else '',
            transactionDate=str(raw_data[3]) if raw_data[3] else '',
            transactionId=str(raw_data[4]) if raw_data[4] else '',
            transactionType=str(raw_data[5]) if raw_data[5] else '',
            serviceChannel=str(raw_data[6]) if raw_data[6] else '',
            tillNumber=str(raw_data[7]) if raw_data[7] else None,
            currency=str(raw_data[8]) if raw_data[8] else '',
            tzsAmount=float(raw_data[9]) if raw_data[9] is not None else 0.0,
            original_timestamp=datetime.now().isoformat()
        )
    
    def create_record_from_dict(self, record_data: dict) -> AgentTransactionRecord:
        """Create AgentTransactionRecord from dictionary (for RabbitMQ consumption)"""
        return AgentTransactionRecord(
            source_table='agentTransactions',
            timestamp_column_value=record_data.get('transactionDate', ''),
            reportingDate=record_data.get('reportingDate', ''),
            agentId=record_data.get('agentId', ''),
            agentStatus=record_data.get('agentStatus', ''),
            transactionDate=record_data.get('transactionDate', ''),
            transactionId=record_data.get('transactionId', ''),
            transactionType=record_data.get('transactionType', ''),
            serviceChannel=record_data.get('serviceChannel', ''),
            tillNumber=record_data.get('tillNumber'),
            currency=record_data.get('currency', ''),
            tzsAmount=float(record_data.get('tzsAmount', 0.0)),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: AgentTransactionRecord, pg_cursor) -> None:
        """Insert Agent Transaction record to PostgreSQL with camelCase table and fields"""
        query = """
        INSERT INTO "agentTransactions" (
            "reportingDate", "agentId", "agentStatus", "transactionDate", "transactionId",
            "transactionType", "serviceChannel", "tillNumber", "currency", "tzsAmount"
        ) VALUES (
            %s, %s, %s, %s::date, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("transactionId") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "agentId" = EXCLUDED."agentId",
            "agentStatus" = EXCLUDED."agentStatus",
            "transactionDate" = EXCLUDED."transactionDate",
            "transactionType" = EXCLUDED."transactionType",
            "serviceChannel" = EXCLUDED."serviceChannel",
            "tillNumber" = EXCLUDED."tillNumber",
            "currency" = EXCLUDED."currency",
            "tzsAmount" = EXCLUDED."tzsAmount"
        """
        
        pg_cursor.execute(query, (
            record.reportingDate,
            record.agentId,
            record.agentStatus,
            record.transactionDate,
            record.transactionId,
            record.transactionType,
            record.serviceChannel,
            record.tillNumber,
            record.currency,
            record.tzsAmount
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for agentTransactions"""
        return """
        INSERT INTO "agentTransactions" (
            "reportingDate", "agentId", "agentStatus", "transactionDate", "transactionId",
            "transactionType", "serviceChannel", "tillNumber", "currency", "tzsAmount"
        ) VALUES (
            %s, %s, %s, %s::date, %s, %s, %s, %s, %s, %s
        )
        """
    
    def validate_record(self, record: AgentTransactionRecord) -> bool:
        """Validate Agent Transaction record"""
        if not super().validate_record(record):
            return False
        
        # Basic validations
        if not record.transactionId:
            return False
        if not record.agentId:
            return False
        if not record.transactionDate:
            return False
            
        return True