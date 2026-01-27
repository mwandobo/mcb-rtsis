"""
Personal Data Corporate processor with camelCase naming
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime, date
from decimal import Decimal
from .base import BaseProcessor, BaseRecord

@dataclass
class PersonalDataCorporateRecord(BaseRecord):
    """Personal Data Corporate record structure with camelCase fields"""
    reportingDate: str
    companyName: Optional[str]
    customerIdentificationNumber: Optional[str]
    establishmentDate: Optional[date]
    legalForm: Optional[str]
    negativeClientStatus: Optional[str]
    numberOfEmployees: Optional[int]
    numberOfEmployeesMale: Optional[int]
    numberOfEmployeesFemale: Optional[int]
    registrationCountry: Optional[str]
    registrationNumber: Optional[str]
    taxIdentificationNumber: Optional[str]
    tradeName: Optional[str]
    parentName: Optional[str]
    parentIncorporationNumber: Optional[str]
    groupId: Optional[str]
    sectorSnaClassification: Optional[str]
    fullName: Optional[str]
    gender: Optional[str]
    cellPhone: Optional[str]
    relationType: Optional[str]
    nationalId: Optional[str]
    appointmentDate: Optional[str]
    terminationDate: Optional[str]
    rateValueOfSharesOwned: Optional[str]
    amountValueOfSharesOwned: Optional[str]
    street: Optional[str]
    country: Optional[str]
    region: Optional[str]
    district: Optional[str]
    ward: Optional[str]
    houseNumber: Optional[str]
    postalCode: Optional[str]
    poBox: Optional[str]
    zipCode: Optional[str]
    primaryPostalCode: Optional[str]
    primaryRegion: Optional[str]
    primaryDistrict: Optional[str]
    primaryWard: Optional[str]
    primaryStreet: Optional[str]
    primaryHouseNumber: Optional[str]
    secondaryStreet: Optional[str]
    secondaryHouseNumber: Optional[str]
    secondaryPostalCode: Optional[str]
    secondaryRegion: Optional[str]
    secondaryDistrict: Optional[str]
    secondaryCountry: Optional[str]
    secondaryTextAddress: Optional[str]
    mobileNumber: Optional[str]
    alternativeMobileNumber: Optional[str]
    fixedLineNumber: Optional[str]
    faxNumber: Optional[str]
    emailAddress: Optional[str]
    socialMedia: Optional[str]
    entityName: Optional[str]
    entityType: Optional[str]
    certificateIncorporation: Optional[str]
    entityRegion: Optional[str]
    entityDistrict: Optional[str]
    entityWard: Optional[str]
    entityStreet: Optional[str]
    entityHouseNumber: Optional[str]
    entityPostalCode: Optional[str]
    groupParentCode: Optional[str]
    shareOwnedPercentage: Optional[str]
    shareOwnedAmount: Optional[str]
    retry_count: int = 0

class PersonalDataCorporateProcessor(BaseProcessor):
    """Processor for personal data corporate with camelCase naming"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> PersonalDataCorporateRecord:
        """Convert raw DB2 data to PersonalDataCorporateRecord"""
        return PersonalDataCorporateRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]) if raw_data[0] else '',  # reportingDate
            reportingDate=str(raw_data[0]) if raw_data[0] else '',
            companyName=str(raw_data[1]).strip() if raw_data[1] else None,
            customerIdentificationNumber=str(raw_data[2]).strip() if raw_data[2] else None,
            establishmentDate=raw_data[3] if isinstance(raw_data[3], date) else None,
            legalForm=str(raw_data[4]).strip() if raw_data[4] else None,
            negativeClientStatus=str(raw_data[5]).strip() if raw_data[5] else None,
            numberOfEmployees=int(raw_data[6]) if raw_data[6] is not None else None,
            numberOfEmployeesMale=int(raw_data[7]) if raw_data[7] is not None else None,
            numberOfEmployeesFemale=int(raw_data[8]) if raw_data[8] is not None else None,
            registrationCountry=str(raw_data[9]).strip() if raw_data[9] else None,
            registrationNumber=str(raw_data[10]).strip() if raw_data[10] else None,
            taxIdentificationNumber=str(raw_data[11]).strip() if raw_data[11] else None,
            tradeName=str(raw_data[12]).strip() if raw_data[12] else None,
            parentName=str(raw_data[13]).strip() if raw_data[13] else None,
            parentIncorporationNumber=str(raw_data[14]).strip() if raw_data[14] else None,
            groupId=str(raw_data[15]).strip() if raw_data[15] else None,
            sectorSnaClassification=str(raw_data[16]).strip() if raw_data[16] else None,
            fullName=str(raw_data[17]).strip() if raw_data[17] else None,
            gender=str(raw_data[18]).strip() if raw_data[18] else None,
            cellPhone=str(raw_data[19]).strip() if raw_data[19] else None,
            relationType=str(raw_data[20]).strip() if raw_data[20] else None,
            nationalId=str(raw_data[21]).strip() if raw_data[21] else None,
            appointmentDate=str(raw_data[22]).strip() if raw_data[22] else None,
            terminationDate=str(raw_data[23]).strip() if raw_data[23] else None,
            rateValueOfSharesOwned=str(raw_data[24]).strip() if raw_data[24] else None,
            amountValueOfSharesOwned=str(raw_data[25]).strip() if raw_data[25] else None,
            street=str(raw_data[26]).strip() if raw_data[26] else None,
            country=str(raw_data[27]).strip() if raw_data[27] else None,
            region=str(raw_data[28]).strip() if raw_data[28] else None,
            district=str(raw_data[29]).strip() if raw_data[29] else None,
            ward=str(raw_data[30]).strip() if raw_data[30] else None,
            houseNumber=str(raw_data[31]).strip() if raw_data[31] else None,
            postalCode=str(raw_data[32]).strip() if raw_data[32] else None,
            poBox=str(raw_data[33]).strip() if raw_data[33] else None,
            zipCode=str(raw_data[34]).strip() if raw_data[34] else None,
            primaryPostalCode=str(raw_data[35]).strip() if raw_data[35] else None,
            primaryRegion=str(raw_data[36]).strip() if raw_data[36] else None,
            primaryDistrict=str(raw_data[37]).strip() if raw_data[37] else None,
            primaryWard=str(raw_data[38]).strip() if raw_data[38] else None,
            primaryStreet=str(raw_data[39]).strip() if raw_data[39] else None,
            primaryHouseNumber=str(raw_data[40]).strip() if raw_data[40] else None,
            secondaryStreet=str(raw_data[41]).strip() if raw_data[41] else None,
            secondaryHouseNumber=str(raw_data[42]).strip() if raw_data[42] else None,
            secondaryPostalCode=str(raw_data[43]).strip() if raw_data[43] else None,
            secondaryRegion=str(raw_data[44]).strip() if raw_data[44] else None,
            secondaryDistrict=str(raw_data[45]).strip() if raw_data[45] else None,
            secondaryCountry=str(raw_data[46]).strip() if raw_data[46] else None,
            secondaryTextAddress=str(raw_data[47]).strip() if raw_data[47] else None,
            mobileNumber=str(raw_data[48]).strip() if raw_data[48] else None,
            alternativeMobileNumber=str(raw_data[49]).strip() if raw_data[49] else None,
            fixedLineNumber=str(raw_data[50]).strip() if raw_data[50] else None,
            faxNumber=str(raw_data[51]).strip() if raw_data[51] else None,
            emailAddress=str(raw_data[52]).strip() if raw_data[52] else None,
            socialMedia=str(raw_data[53]).strip() if raw_data[53] else None,
            entityName=str(raw_data[54]).strip() if raw_data[54] else None,
            entityType=str(raw_data[55]).strip() if raw_data[55] else None,
            certificateIncorporation=str(raw_data[56]).strip() if raw_data[56] else None,
            entityRegion=str(raw_data[57]).strip() if raw_data[57] else None,
            entityDistrict=str(raw_data[58]).strip() if raw_data[58] else None,
            entityWard=str(raw_data[59]).strip() if raw_data[59] else None,
            entityStreet=str(raw_data[60]).strip() if raw_data[60] else None,
            entityHouseNumber=str(raw_data[61]).strip() if raw_data[61] else None,
            entityPostalCode=str(raw_data[62]).strip() if raw_data[62] else None,
            groupParentCode=str(raw_data[63]).strip() if raw_data[63] else None,
            shareOwnedPercentage=str(raw_data[64]).strip() if raw_data[64] else None,
            shareOwnedAmount=str(raw_data[65]).strip() if raw_data[65] else None,
            # Note: we have exactly 66 columns (0-65), no cursor tracking needed
            original_timestamp=datetime.now().isoformat()
        )
    
    def create_record_from_dict(self, record_data: dict) -> PersonalDataCorporateRecord:
        """Create PersonalDataCorporateRecord from dictionary (for RabbitMQ consumption)"""
        return PersonalDataCorporateRecord(
            source_table='personalDataCorporate',
            timestamp_column_value=record_data.get('reportingDate', ''),
            reportingDate=record_data.get('reportingDate', ''),
            companyName=record_data.get('companyName'),
            customerIdentificationNumber=record_data.get('customerIdentificationNumber'),
            establishmentDate=datetime.fromisoformat(record_data.get('establishmentDate')).date() if record_data.get('establishmentDate') else None,
            legalForm=record_data.get('legalForm'),
            negativeClientStatus=record_data.get('negativeClientStatus'),
            numberOfEmployees=int(record_data.get('numberOfEmployees')) if record_data.get('numberOfEmployees') is not None else None,
            numberOfEmployeesMale=int(record_data.get('numberOfEmployeesMale')) if record_data.get('numberOfEmployeesMale') is not None else None,
            numberOfEmployeesFemale=int(record_data.get('numberOfEmployeesFemale')) if record_data.get('numberOfEmployeesFemale') is not None else None,
            registrationCountry=record_data.get('registrationCountry'),
            registrationNumber=record_data.get('registrationNumber'),
            taxIdentificationNumber=record_data.get('taxIdentificationNumber'),
            tradeName=record_data.get('tradeName'),
            parentName=record_data.get('parentName'),
            parentIncorporationNumber=record_data.get('parentIncorporationNumber'),
            groupId=record_data.get('groupId'),
            sectorSnaClassification=record_data.get('sectorSnaClassification'),
            fullName=record_data.get('fullName'),
            gender=record_data.get('gender'),
            cellPhone=record_data.get('cellPhone'),
            relationType=record_data.get('relationType'),
            nationalId=record_data.get('nationalId'),
            appointmentDate=record_data.get('appointmentDate'),
            terminationDate=record_data.get('terminationDate'),
            rateValueOfSharesOwned=record_data.get('rateValueOfSharesOwned'),
            amountValueOfSharesOwned=record_data.get('amountValueOfSharesOwned'),
            street=record_data.get('street'),
            country=record_data.get('country'),
            region=record_data.get('region'),
            district=record_data.get('district'),
            ward=record_data.get('ward'),
            houseNumber=record_data.get('houseNumber'),
            postalCode=record_data.get('postalCode'),
            poBox=record_data.get('poBox'),
            zipCode=record_data.get('zipCode'),
            primaryPostalCode=record_data.get('primaryPostalCode'),
            primaryRegion=record_data.get('primaryRegion'),
            primaryDistrict=record_data.get('primaryDistrict'),
            primaryWard=record_data.get('primaryWard'),
            primaryStreet=record_data.get('primaryStreet'),
            primaryHouseNumber=record_data.get('primaryHouseNumber'),
            secondaryStreet=record_data.get('secondaryStreet'),
            secondaryHouseNumber=record_data.get('secondaryHouseNumber'),
            secondaryPostalCode=record_data.get('secondaryPostalCode'),
            secondaryRegion=record_data.get('secondaryRegion'),
            secondaryDistrict=record_data.get('secondaryDistrict'),
            secondaryCountry=record_data.get('secondaryCountry'),
            secondaryTextAddress=record_data.get('secondaryTextAddress'),
            mobileNumber=record_data.get('mobileNumber'),
            alternativeMobileNumber=record_data.get('alternativeMobileNumber'),
            fixedLineNumber=record_data.get('fixedLineNumber'),
            faxNumber=record_data.get('faxNumber'),
            emailAddress=record_data.get('emailAddress'),
            socialMedia=record_data.get('socialMedia'),
            entityName=record_data.get('entityName'),
            entityType=record_data.get('entityType'),
            certificateIncorporation=record_data.get('certificateIncorporation'),
            entityRegion=record_data.get('entityRegion'),
            entityDistrict=record_data.get('entityDistrict'),
            entityWard=record_data.get('entityWard'),
            entityStreet=record_data.get('entityStreet'),
            entityHouseNumber=record_data.get('entityHouseNumber'),
            entityPostalCode=record_data.get('entityPostalCode'),
            groupParentCode=record_data.get('groupParentCode'),
            shareOwnedPercentage=record_data.get('shareOwnedPercentage'),
            shareOwnedAmount=record_data.get('shareOwnedAmount'),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: PersonalDataCorporateRecord, pg_cursor) -> None:
        """Insert Personal Data Corporate record to PostgreSQL with camelCase table and fields"""
        query = """
        INSERT INTO "personalDataCorporate" (
            "reportingDate", "companyName", "customerIdentificationNumber", "establishmentDate", "legalForm",
            "negativeClientStatus", "numberOfEmployees", "numberOfEmployeesMale", "numberOfEmployeesFemale",
            "registrationCountry", "registrationNumber", "taxIdentificationNumber", "tradeName", "parentName",
            "parentIncorporationNumber", "groupId", "sectorSnaClassification", "fullName", "gender", "cellPhone",
            "relationType", "nationalId", "appointmentDate", "terminationDate", "rateValueOfSharesOwned",
            "amountValueOfSharesOwned", "street", "country", "region", "district", "ward", "houseNumber",
            "postalCode", "poBox", "zipCode", "primaryPostalCode", "primaryRegion", "primaryDistrict",
            "primaryWard", "primaryStreet", "primaryHouseNumber", "secondaryStreet", "secondaryHouseNumber",
            "secondaryPostalCode", "secondaryRegion", "secondaryDistrict", "secondaryCountry", "secondaryTextAddress",
            "mobileNumber", "alternativeMobileNumber", "fixedLineNumber", "faxNumber", "emailAddress", "socialMedia",
            "entityName", "entityType", "certificateIncorporation", "entityRegion", "entityDistrict", "entityWard",
            "entityStreet", "entityHouseNumber", "entityPostalCode", "groupParentCode", "shareOwnedPercentage",
            "shareOwnedAmount"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        """
        
        pg_cursor.execute(query, (
            record.reportingDate, record.companyName, record.customerIdentificationNumber,
            record.establishmentDate, record.legalForm, record.negativeClientStatus,
            record.numberOfEmployees, record.numberOfEmployeesMale, record.numberOfEmployeesFemale,
            record.registrationCountry, record.registrationNumber, record.taxIdentificationNumber,
            record.tradeName, record.parentName, record.parentIncorporationNumber, record.groupId,
            record.sectorSnaClassification, record.fullName, record.gender, record.cellPhone,
            record.relationType, record.nationalId, record.appointmentDate, record.terminationDate,
            record.rateValueOfSharesOwned, record.amountValueOfSharesOwned, record.street, record.country,
            record.region, record.district, record.ward, record.houseNumber, record.postalCode,
            record.poBox, record.zipCode, record.primaryPostalCode, record.primaryRegion,
            record.primaryDistrict, record.primaryWard, record.primaryStreet, record.primaryHouseNumber,
            record.secondaryStreet, record.secondaryHouseNumber, record.secondaryPostalCode,
            record.secondaryRegion, record.secondaryDistrict, record.secondaryCountry,
            record.secondaryTextAddress, record.mobileNumber, record.alternativeMobileNumber,
            record.fixedLineNumber, record.faxNumber, record.emailAddress, record.socialMedia,
            record.entityName, record.entityType, record.certificateIncorporation, record.entityRegion,
            record.entityDistrict, record.entityWard, record.entityStreet, record.entityHouseNumber,
            record.entityPostalCode, record.groupParentCode, record.shareOwnedPercentage,
            record.shareOwnedAmount
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for personal data corporate"""
        return self.insert_to_postgres.__doc__  # Same as insert for now
    
    def validate_record(self, record: PersonalDataCorporateRecord) -> bool:
        """Validate Personal Data Corporate record"""
        if not super().validate_record(record):
            return False
        
        # Basic validations
        if not record.customerIdentificationNumber:
            return False
            
        return True