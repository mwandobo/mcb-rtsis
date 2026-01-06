"""
Agent information processor
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class AgentRecord(BaseRecord):
    """Agent information record structure"""
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
    cert_incorporation: str
    nationality: str
    agent_status: str
    agent_type: str
    account_number: str
    region: str
    district: str
    ward: str
    street: str
    house_number: str
    postal_code: str
    country: str
    gps_coordinates: str
    agent_tax_identification_number: str
    business_license: str
    last_modified: str
    retry_count: int = 0

class AgentProcessor(BaseProcessor):
    """Processor for agent information data"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> AgentRecord:
        """Convert raw DB2 data to AgentRecord - Updated for agents-from-agents-list-NEW-V1.table.sql"""
        # raw_data structure from agents-from-agents-list-NEW-V1.table.sql (24 fields):
        # 0: reportingDate, 1: agentName, 2: agentId, 3: tillNumber, 4: businessForm,
        # 5: agentPrincipal, 6: agentPrincipalName, 7: gender, 8: registrationDate,
        # 9: closedDate, 10: certIncorporation, 11: nationality, 12: agentStatus,
        # 13: agentType, 14: accountNumber, 15: region, 16: district, 17: ward,
        # 18: street, 19: houseNumber, 20: postalCode, 21: country, 22: gpsCoordinates,
        # 23: agentTaxIdentificationNumber, 24: businessLicense
        
        return AgentRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[8]),  # registrationDate for tracking
            reporting_date=str(raw_data[0]),
            agent_name=str(raw_data[1]).strip(),
            agent_id=str(raw_data[2]),
            till_number=str(raw_data[3]) if raw_data[3] else None,
            business_form=str(raw_data[4]),
            agent_principal=str(raw_data[5]),
            agent_principal_name=str(raw_data[6]).strip(),
            gender=str(raw_data[7]),
            registration_date=str(raw_data[8]),
            closed_date=str(raw_data[9]) if raw_data[9] else None,
            cert_incorporation=str(raw_data[10]),
            nationality=str(raw_data[11]),
            agent_status=str(raw_data[12]),
            agent_type=str(raw_data[13]),
            account_number=str(raw_data[14]) if raw_data[14] else None,
            region=str(raw_data[15]),
            district=str(raw_data[16]),
            ward=str(raw_data[17]),
            street=str(raw_data[18]),
            house_number=str(raw_data[19]),
            postal_code=str(raw_data[20]),
            country=str(raw_data[21]),
            gps_coordinates=str(raw_data[22]) if raw_data[22] else None,
            agent_tax_identification_number=str(raw_data[23]),
            business_license=str(raw_data[24]) if len(raw_data) > 24 and raw_data[24] else None,
            last_modified=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
    
    def _convert_timestamp_to_postgres(self, timestamp_value) -> str:
        """Convert DB2 timestamp to PostgreSQL format"""
        try:
            if timestamp_value is None:
                return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # If it's already a datetime object
            if hasattr(timestamp_value, 'strftime'):
                return timestamp_value.strftime('%Y-%m-%d %H:%M:%S')
            
            # If it's a string, try to parse it
            timestamp_str = str(timestamp_value)
            if timestamp_str and timestamp_str != 'None':
                # Try different timestamp formats
                for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
                    try:
                        dt = datetime.strptime(timestamp_str.split('.')[0], fmt.split('.')[0])
                        return dt.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        continue
            
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def insert_to_postgres(self, record: AgentRecord, pg_cursor) -> None:
        """Insert agent record to PostgreSQL"""
        query = self.get_upsert_query()
        
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
            record.business_license,
            record.last_modified
        ))
    
    def get_upsert_query(self) -> str:
        """Get insert query for agent information with camelCase"""
        return """
        INSERT INTO "agents" (
            "reportingDate", "agentName", "agentId", "tillNumber", "businessForm",
            "agentPrincipal", "agentPrincipalName", "gender", "registrationDate",
            "closedDate", "certIncorporation", "nationality", "agentStatus",
            "agentType", "accountNumber", "region", "district", "ward", "street",
            "houseNumber", "postalCode", "country", "gpsCoordinates",
            "agentTaxIdentificationNumber", "businessLicense", "lastModified"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
            "businessLicense" = EXCLUDED."businessLicense",
            "lastModified" = EXCLUDED."lastModified"
        """
    
    def validate_record(self, record: AgentRecord) -> bool:
        """Validate agent record"""
        if not super().validate_record(record):
            return False
        
        # Agent-specific validations - be more lenient with agent_id
        if not record.agent_name or record.agent_name.strip() == '':
            return False
        if not record.agent_status or record.agent_status.strip() == '':
            return False
        if not record.agent_type or record.agent_type.strip() == '':
            return False
        
        # Allow records with any agent_id (including #N/A, 0, etc.)
        # The upsert will handle duplicates
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform agent data"""
        # Add any agent-specific transformations here
        return {
            'agent_id_normalized': str(raw_data[2]).strip().upper(),
            'agent_name_normalized': str(raw_data[1]).strip(),
            'status_validated': str(raw_data[12]).strip() if raw_data[12] else 'Unknown'
        }