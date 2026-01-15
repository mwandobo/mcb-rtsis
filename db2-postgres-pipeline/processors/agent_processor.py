"""
Simple Agent processor
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class AgentRecord(BaseRecord):
    """Agent record structure"""
    reporting_date: str
    agent_name: str
    agent_id: str
    till_number: Optional[str]
    business_form: str
    agent_principal: str
    agent_principal_name: str
    gender: str
    registration_date: str
    closed_date: Optional[str]
    cert_incorporation: Optional[str]
    nationality: str
    agent_status: str
    agent_type: str
    account_number: Optional[str]
    region: str
    district: str
    ward: str
    street: str
    house_number: str
    postal_code: str
    country: str
    gps_coordinates: Optional[str]
    agent_tax_identification_number: Optional[str]
    business_license: Optional[str]
    retry_count: int = 0

class AgentProcessor(BaseProcessor):
    """Simple processor for agent data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> AgentRecord:
        """Convert raw DB2 data to AgentRecord"""
        return AgentRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]) if raw_data[0] else '',
            reporting_date=str(raw_data[0]) if raw_data[0] else '',
            agent_name=str(raw_data[1]) if raw_data[1] else '',
            agent_id=str(raw_data[2]) if raw_data[2] else '',
            till_number=str(raw_data[3]) if raw_data[3] else None,
            business_form=str(raw_data[4]) if raw_data[4] else '',
            agent_principal=str(raw_data[5]) if raw_data[5] else '',
            agent_principal_name=str(raw_data[6]) if raw_data[6] else '',
            gender=str(raw_data[7]) if raw_data[7] else '',
            registration_date=str(raw_data[8]) if raw_data[8] else '',
            closed_date=str(raw_data[9]) if raw_data[9] else None,
            cert_incorporation=str(raw_data[10]) if raw_data[10] else None,
            nationality=str(raw_data[11]) if raw_data[11] else '',
            agent_status=str(raw_data[12]) if raw_data[12] else '',
            agent_type=str(raw_data[13]) if raw_data[13] else '',
            account_number=str(raw_data[14]) if raw_data[14] else None,
            region=str(raw_data[15]) if raw_data[15] else '',
            district=str(raw_data[16]) if raw_data[16] else '',
            ward=str(raw_data[17]) if raw_data[17] else '',
            street=str(raw_data[18]) if raw_data[18] else '',
            house_number=str(raw_data[19]) if raw_data[19] else '',
            postal_code=str(raw_data[20]) if raw_data[20] else '',
            country=str(raw_data[21]) if raw_data[21] else '',
            gps_coordinates=str(raw_data[22]) if raw_data[22] else None,
            agent_tax_identification_number=str(raw_data[23]) if raw_data[23] else None,
            business_license=str(raw_data[24]) if raw_data[24] else None,
            original_timestamp=datetime.now().isoformat()
        )
    
    def create_record_from_dict(self, record_data: dict) -> AgentRecord:
        """Create AgentRecord from dictionary (for RabbitMQ consumption)"""
        return AgentRecord(
            source_table='agents',
            timestamp_column_value=record_data.get('reportingDate', ''),
            reporting_date=record_data.get('reportingDate', ''),
            agent_name=record_data.get('agentName', ''),
            agent_id=record_data.get('agentId', ''),
            till_number=record_data.get('tillNumber'),
            business_form=record_data.get('businessForm', ''),
            agent_principal=record_data.get('agentPrincipal', ''),
            agent_principal_name=record_data.get('agentPrincipalName', ''),
            gender=record_data.get('gender', ''),
            registration_date=record_data.get('registrationDate', ''),
            closed_date=record_data.get('closedDate'),
            cert_incorporation=record_data.get('certIncorporation'),
            nationality=record_data.get('nationality', ''),
            agent_status=record_data.get('agentStatus', ''),
            agent_type=record_data.get('agentType', ''),
            account_number=record_data.get('accountNumber'),
            region=record_data.get('region', ''),
            district=record_data.get('district', ''),
            ward=record_data.get('ward', ''),
            street=record_data.get('street', ''),
            house_number=record_data.get('houseNumber', ''),
            postal_code=record_data.get('postalCode', ''),
            country=record_data.get('country', ''),
            gps_coordinates=record_data.get('gpsCoordinates'),
            agent_tax_identification_number=record_data.get('agentTaxIdentificationNumber'),
            business_license=record_data.get('businessLicense'),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: AgentRecord, pg_cursor) -> None:
        """Insert Agent record to PostgreSQL with upsert logic"""
        query = """
        INSERT INTO "agents" (
            "reportingDate", "agentName", "agentId", "tillNumber", "businessForm",
            "agentPrincipal", "agentPrincipalName", "gender", "registrationDate", "closedDate",
            "certIncorporation", "nationality", "agentStatus", "agentType", "accountNumber",
            "region", "district", "ward", "street", "houseNumber", "postalCode", "country",
            "gpsCoordinates", "agentTaxIdentificationNumber", "businessLicense"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("agentId") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "agentName" = EXCLUDED."agentName",
            "tillNumber" = EXCLUDED."tillNumber",
            "businessForm" = EXCLUDED."businessForm",
            "agentPrincipal" = EXCLUDED."agentPrincipal",
            "agentPrincipalName" = EXCLUDED."agentPrincipalName",
            "gender" = EXCLUDED."gender",
            "registrationDate" = EXCLUDED."registrationDate",
            "closedDate" = EXCLUDED."closedDate",
            "certIncorporation" = EXCLUDED."certIncorporation",
            "nationality" = EXCLUDED."nationality",
            "agentStatus" = EXCLUDED."agentStatus",
            "agentType" = EXCLUDED."agentType",
            "accountNumber" = EXCLUDED."accountNumber",
            "region" = EXCLUDED."region",
            "district" = EXCLUDED."district",
            "ward" = EXCLUDED."ward",
            "street" = EXCLUDED."street",
            "houseNumber" = EXCLUDED."houseNumber",
            "postalCode" = EXCLUDED."postalCode",
            "country" = EXCLUDED."country",
            "gpsCoordinates" = EXCLUDED."gpsCoordinates",
            "agentTaxIdentificationNumber" = EXCLUDED."agentTaxIdentificationNumber",
            "businessLicense" = EXCLUDED."businessLicense"
        """
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.agent_name,
            record.agent_id,
            record.till_number,
            record.business_form,
            record.agent_principal,
            record.agent_principal_name,
            record.gender,
            record.registration_date,
            record.closed_date,
            record.cert_incorporation,
            record.nationality,
            record.agent_status,
            record.agent_type,
            record.account_number,
            record.region,
            record.district,
            record.ward,
            record.street,
            record.house_number,
            record.postal_code,
            record.country,
            record.gps_coordinates,
            record.agent_tax_identification_number,
            record.business_license
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for agents"""
        return """
        INSERT INTO "agents" (
            "reportingDate", "agentName", "agentId", "tillNumber", "businessForm",
            "agentPrincipal", "agentPrincipalName", "gender", "registrationDate", "closedDate",
            "certIncorporation", "nationality", "agentStatus", "agentType", "accountNumber",
            "region", "district", "ward", "street", "houseNumber", "postalCode", "country",
            "gpsCoordinates", "agentTaxIdentificationNumber", "businessLicense"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
    
    def validate_record(self, record: AgentRecord) -> bool:
        """Validate Agent record"""
        if not super().validate_record(record):
            return False
        
        # Basic validations
        if not record.agent_id:
            return False
        if not record.agent_name:
            return False
            
        return True