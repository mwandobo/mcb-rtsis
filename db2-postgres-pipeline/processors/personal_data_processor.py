"""
Personal Data processor with camelCase naming
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime, date
from decimal import Decimal
from .base import BaseProcessor, BaseRecord

@dataclass
class PersonalDataRecord(BaseRecord):
    """Personal Data record structure with camelCase fields"""
    reportingDate: str
    customerIdentificationNumber: Optional[str]
    firstName: Optional[str]
    middleNames: Optional[str]
    otherNames: Optional[str]
    fullNames: Optional[str]
    presentSurname: Optional[str]
    birthSurname: Optional[str]
    gender: Optional[str]
    maritalStatus: Optional[str]
    numberSpouse: Optional[str]
    nationality: Optional[str]
    citizenship: Optional[str]
    residency: Optional[str]
    profession: Optional[str]
    sectorSnaClassification: Optional[str]
    fateStatus: Optional[str]
    socialStatus: Optional[str]
    employmentStatus: Optional[str]
    monthlyIncome: Optional[Decimal]
    numberDependants: Optional[int]
    educationLevel: Optional[str]
    averageMonthlyExpenditure: Optional[Decimal]
    negativeClientStatus: Optional[str]
    spousesFullName: Optional[str]
    spouseIdentificationType: Optional[str]
    spouseIdentificationNumber: Optional[str]
    maidenName: Optional[str]
    monthlyExpenses: Optional[Decimal]
    birthDate: Optional[date]
    birthCountry: Optional[str]
    birthPostalCode: Optional[str]
    birthHouseNumber: Optional[str]
    birthRegion: Optional[str]
    birthDistrict: Optional[str]
    birthWard: Optional[str]
    birthStreet: Optional[str]
    identificationType: Optional[str]
    identificationNumber: Optional[str]
    issuanceDate: Optional[str]
    expirationDate: Optional[str]
    issuancePlace: Optional[str]
    issuingAuthority: Optional[str]
    businessName: Optional[str]
    establishmentDate: Optional[date]
    businessRegistrationNumber: Optional[str]
    businessRegistrationDate: Optional[date]
    businessLicenseNumber: Optional[str]
    taxIdentificationNumber: Optional[str]
    employerName: Optional[str]
    employerRegion: Optional[str]
    employerDistrict: Optional[str]
    employerWard: Optional[str]
    employerStreet: Optional[str]
    employerHouseNumber: Optional[str]
    employerPostalCode: Optional[str]
    businessNature: Optional[str]
    mobileNumber: Optional[str]
    alternativeMobileNumber: Optional[str]
    fixedLineNumber: Optional[str]
    faxNumber: Optional[str]
    emailAddress: Optional[str]
    socialMedia: Optional[str]
    mainAddress: Optional[str]
    street: Optional[str]
    houseNumber: Optional[str]
    postalCode: Optional[str]
    region: Optional[str]
    district: Optional[str]
    ward: Optional[str]
    country: Optional[str]
    sstreet: Optional[str]
    shouseNumber: Optional[str]
    spostalCode: Optional[str]
    sregion: Optional[str]
    sdistrict: Optional[str]
    sward: Optional[str]
    scountry: Optional[str]
    retry_count: int = 0

class PersonalDataProcessor(BaseProcessor):
    """Processor for personal data with camelCase naming"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> PersonalDataRecord:
        """Convert raw DB2 data to PersonalDataRecord"""
        return PersonalDataRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]) if raw_data[0] else '',  # reportingDate
            reportingDate=str(raw_data[0]) if raw_data[0] else '',
            customerIdentificationNumber=str(raw_data[1]).strip() if raw_data[1] else None,
            firstName=str(raw_data[2]).strip() if raw_data[2] else None,
            middleNames=str(raw_data[3]).strip() if raw_data[3] else None,
            otherNames=str(raw_data[4]).strip() if raw_data[4] else None,
            fullNames=str(raw_data[5]).strip() if raw_data[5] else None,
            presentSurname=str(raw_data[6]).strip() if raw_data[6] else None,
            birthSurname=str(raw_data[7]).strip() if raw_data[7] else None,
            gender=str(raw_data[8]).strip() if raw_data[8] else None,
            maritalStatus=str(raw_data[9]).strip() if raw_data[9] else None,
            numberSpouse=str(raw_data[10]).strip() if raw_data[10] else None,
            nationality=str(raw_data[11]).strip() if raw_data[11] else None,
            citizenship=str(raw_data[12]).strip() if raw_data[12] else None,
            residency=str(raw_data[13]).strip() if raw_data[13] else None,
            profession=str(raw_data[14]).strip() if raw_data[14] else None,
            sectorSnaClassification=str(raw_data[15]).strip() if raw_data[15] else None,
            fateStatus=str(raw_data[16]).strip() if raw_data[16] else None,
            socialStatus=str(raw_data[17]).strip() if raw_data[17] else None,
            employmentStatus=str(raw_data[18]).strip() if raw_data[18] else None,
            monthlyIncome=Decimal(str(raw_data[19])) if raw_data[19] is not None else None,
            numberDependants=int(raw_data[20]) if raw_data[20] is not None else None,
            educationLevel=str(raw_data[21]).strip() if raw_data[21] else None,
            averageMonthlyExpenditure=Decimal(str(raw_data[22])) if raw_data[22] is not None else None,
            negativeClientStatus=str(raw_data[23]).strip() if raw_data[23] else None,
            spousesFullName=str(raw_data[24]).strip() if raw_data[24] else None,
            spouseIdentificationType=str(raw_data[25]).strip() if raw_data[25] else None,
            spouseIdentificationNumber=str(raw_data[26]).strip() if raw_data[26] else None,
            maidenName=str(raw_data[27]).strip() if raw_data[27] else None,
            monthlyExpenses=Decimal(str(raw_data[28])) if raw_data[28] is not None else None,
            birthDate=raw_data[29] if isinstance(raw_data[29], date) else None,
            birthCountry=str(raw_data[30]).strip() if raw_data[30] else None,
            birthPostalCode=str(raw_data[31]).strip() if raw_data[31] else None,
            birthHouseNumber=str(raw_data[32]).strip() if raw_data[32] else None,
            birthRegion=str(raw_data[33]).strip() if raw_data[33] else None,
            birthDistrict=str(raw_data[34]).strip() if raw_data[34] else None,
            birthWard=str(raw_data[35]).strip() if raw_data[35] else None,
            birthStreet=str(raw_data[36]).strip() if raw_data[36] else None,
            identificationType=str(raw_data[37]).strip() if raw_data[37] else None,
            identificationNumber=str(raw_data[38]).strip() if raw_data[38] else None,
            issuanceDate=str(raw_data[39]).strip() if raw_data[39] else None,
            expirationDate=str(raw_data[40]).strip() if raw_data[40] else None,
            issuancePlace=str(raw_data[41]).strip() if raw_data[41] else None,
            issuingAuthority=str(raw_data[42]).strip() if raw_data[42] else None,
            businessName=str(raw_data[43]).strip() if raw_data[43] else None,
            establishmentDate=raw_data[44] if isinstance(raw_data[44], date) else None,
            businessRegistrationNumber=str(raw_data[45]).strip() if raw_data[45] else None,
            businessRegistrationDate=raw_data[46] if isinstance(raw_data[46], date) else None,
            businessLicenseNumber=str(raw_data[47]).strip() if raw_data[47] else None,
            taxIdentificationNumber=str(raw_data[48]).strip() if raw_data[48] else None,
            employerName=str(raw_data[49]).strip() if raw_data[49] else None,
            employerRegion=str(raw_data[50]).strip() if raw_data[50] else None,
            employerDistrict=str(raw_data[51]).strip() if raw_data[51] else None,
            employerWard=str(raw_data[52]).strip() if raw_data[52] else None,
            employerStreet=str(raw_data[53]).strip() if raw_data[53] else None,
            employerHouseNumber=str(raw_data[54]).strip() if raw_data[54] else None,
            employerPostalCode=str(raw_data[55]).strip() if raw_data[55] else None,
            businessNature=str(raw_data[56]).strip() if raw_data[56] else None,
            mobileNumber=str(raw_data[57]).strip() if raw_data[57] else None,
            alternativeMobileNumber=str(raw_data[58]).strip() if raw_data[58] else None,
            fixedLineNumber=str(raw_data[59]).strip() if raw_data[59] else None,
            faxNumber=str(raw_data[60]).strip() if raw_data[60] else None,
            emailAddress=str(raw_data[61]).strip() if raw_data[61] else None,
            socialMedia=str(raw_data[62]).strip() if raw_data[62] else None,
            mainAddress=str(raw_data[63]).strip() if raw_data[63] else None,
            street=str(raw_data[64]).strip() if raw_data[64] else None,
            houseNumber=str(raw_data[65]).strip() if raw_data[65] else None,
            postalCode=str(raw_data[66]).strip() if raw_data[66] else None,
            region=str(raw_data[67]).strip() if raw_data[67] else None,
            district=str(raw_data[68]).strip() if raw_data[68] else None,
            ward=str(raw_data[69]).strip() if raw_data[69] else None,
            country=str(raw_data[70]).strip() if raw_data[70] else None,
            sstreet=str(raw_data[71]).strip() if raw_data[71] else None,
            shouseNumber=str(raw_data[72]).strip() if raw_data[72] else None,
            spostalCode=str(raw_data[73]).strip() if raw_data[73] else None,
            sregion=str(raw_data[74]).strip() if raw_data[74] else None,
            sdistrict=str(raw_data[75]).strip() if raw_data[75] else None,
            sward=str(raw_data[76]).strip() if raw_data[76] else None,
            scountry=str(raw_data[77]).strip() if raw_data[77] else None,
            # Note: raw_data[78] would be cust_id for cursor tracking
            original_timestamp=datetime.now().isoformat()
        )
    
    def create_record_from_dict(self, record_data: dict) -> PersonalDataRecord:
        """Create PersonalDataRecord from dictionary (for RabbitMQ consumption)"""
        return PersonalDataRecord(
            source_table='personalData',
            timestamp_column_value=record_data.get('reportingDate', ''),
            reportingDate=record_data.get('reportingDate', ''),
            customerIdentificationNumber=record_data.get('customerIdentificationNumber'),
            firstName=record_data.get('firstName'),
            middleNames=record_data.get('middleNames'),
            otherNames=record_data.get('otherNames'),
            fullNames=record_data.get('fullNames'),
            presentSurname=record_data.get('presentSurname'),
            birthSurname=record_data.get('birthSurname'),
            gender=record_data.get('gender'),
            maritalStatus=record_data.get('maritalStatus'),
            numberSpouse=record_data.get('numberSpouse'),
            nationality=record_data.get('nationality'),
            citizenship=record_data.get('citizenship'),
            residency=record_data.get('residency'),
            profession=record_data.get('profession'),
            sectorSnaClassification=record_data.get('sectorSnaClassification'),
            fateStatus=record_data.get('fateStatus'),
            socialStatus=record_data.get('socialStatus'),
            employmentStatus=record_data.get('employmentStatus'),
            monthlyIncome=Decimal(str(record_data.get('monthlyIncome'))) if record_data.get('monthlyIncome') is not None else None,
            numberDependants=int(record_data.get('numberDependants')) if record_data.get('numberDependants') is not None else None,
            educationLevel=record_data.get('educationLevel'),
            averageMonthlyExpenditure=Decimal(str(record_data.get('averageMonthlyExpenditure'))) if record_data.get('averageMonthlyExpenditure') is not None else None,
            negativeClientStatus=record_data.get('negativeClientStatus'),
            spousesFullName=record_data.get('spousesFullName'),
            spouseIdentificationType=record_data.get('spouseIdentificationType'),
            spouseIdentificationNumber=record_data.get('spouseIdentificationNumber'),
            maidenName=record_data.get('maidenName'),
            monthlyExpenses=Decimal(str(record_data.get('monthlyExpenses'))) if record_data.get('monthlyExpenses') is not None else None,
            birthDate=datetime.fromisoformat(record_data.get('birthDate')).date() if record_data.get('birthDate') else None,
            birthCountry=record_data.get('birthCountry'),
            birthPostalCode=record_data.get('birthPostalCode'),
            birthHouseNumber=record_data.get('birthHouseNumber'),
            birthRegion=record_data.get('birthRegion'),
            birthDistrict=record_data.get('birthDistrict'),
            birthWard=record_data.get('birthWard'),
            birthStreet=record_data.get('birthStreet'),
            identificationType=record_data.get('identificationType'),
            identificationNumber=record_data.get('identificationNumber'),
            issuanceDate=record_data.get('issuanceDate'),
            expirationDate=record_data.get('expirationDate'),
            issuancePlace=record_data.get('issuancePlace'),
            issuingAuthority=record_data.get('issuingAuthority'),
            businessName=record_data.get('businessName'),
            establishmentDate=datetime.fromisoformat(record_data.get('establishmentDate')).date() if record_data.get('establishmentDate') else None,
            businessRegistrationNumber=record_data.get('businessRegistrationNumber'),
            businessRegistrationDate=datetime.fromisoformat(record_data.get('businessRegistrationDate')).date() if record_data.get('businessRegistrationDate') else None,
            businessLicenseNumber=record_data.get('businessLicenseNumber'),
            taxIdentificationNumber=record_data.get('taxIdentificationNumber'),
            employerName=record_data.get('employerName'),
            employerRegion=record_data.get('employerRegion'),
            employerDistrict=record_data.get('employerDistrict'),
            employerWard=record_data.get('employerWard'),
            employerStreet=record_data.get('employerStreet'),
            employerHouseNumber=record_data.get('employerHouseNumber'),
            employerPostalCode=record_data.get('employerPostalCode'),
            businessNature=record_data.get('businessNature'),
            mobileNumber=record_data.get('mobileNumber'),
            alternativeMobileNumber=record_data.get('alternativeMobileNumber'),
            fixedLineNumber=record_data.get('fixedLineNumber'),
            faxNumber=record_data.get('faxNumber'),
            emailAddress=record_data.get('emailAddress'),
            socialMedia=record_data.get('socialMedia'),
            mainAddress=record_data.get('mainAddress'),
            street=record_data.get('street'),
            houseNumber=record_data.get('houseNumber'),
            postalCode=record_data.get('postalCode'),
            region=record_data.get('region'),
            district=record_data.get('district'),
            ward=record_data.get('ward'),
            country=record_data.get('country'),
            sstreet=record_data.get('sstreet'),
            shouseNumber=record_data.get('shouseNumber'),
            spostalCode=record_data.get('spostalCode'),
            sregion=record_data.get('sregion'),
            sdistrict=record_data.get('sdistrict'),
            sward=record_data.get('sward'),
            scountry=record_data.get('scountry'),
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: PersonalDataRecord, pg_cursor) -> None:
        """Insert Personal Data record to PostgreSQL with camelCase table and fields"""
        query = """
        INSERT INTO "personalData" (
            "reportingDate", "customerIdentificationNumber", "firstName", "middleNames", "otherNames",
            "fullNames", "presentSurname", "birthSurname", "gender", "maritalStatus", "numberSpouse",
            "nationality", "citizenship", "residency", "profession", "sectorSnaClassification",
            "fateStatus", "socialStatus", "employmentStatus", "monthlyIncome", "numberDependants",
            "educationLevel", "averageMonthlyExpenditure", "negativeClientStatus", "spousesFullName",
            "spouseIdentificationType", "spouseIdentificationNumber", "maidenName", "monthlyExpenses",
            "birthDate", "birthCountry", "birthPostalCode", "birthHouseNumber", "birthRegion",
            "birthDistrict", "birthWard", "birthStreet", "identificationType", "identificationNumber",
            "issuanceDate", "expirationDate", "issuancePlace", "issuingAuthority", "businessName",
            "establishmentDate", "businessRegistrationNumber", "businessRegistrationDate",
            "businessLicenseNumber", "taxIdentificationNumber", "employerName", "employerRegion",
            "employerDistrict", "employerWard", "employerStreet", "employerHouseNumber",
            "employerPostalCode", "businessNature", "mobileNumber", "alternativeMobileNumber",
            "fixedLineNumber", "faxNumber", "emailAddress", "socialMedia", "mainAddress", "street",
            "houseNumber", "postalCode", "region", "district", "ward", "country", "sstreet",
            "shouseNumber", "spostalCode", "sregion", "sdistrict", "sward", "scountry"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        pg_cursor.execute(query, (
            record.reportingDate, record.customerIdentificationNumber, record.firstName,
            record.middleNames, record.otherNames, record.fullNames, record.presentSurname,
            record.birthSurname, record.gender, record.maritalStatus, record.numberSpouse,
            record.nationality, record.citizenship, record.residency, record.profession,
            record.sectorSnaClassification, record.fateStatus, record.socialStatus,
            record.employmentStatus, record.monthlyIncome, record.numberDependants,
            record.educationLevel, record.averageMonthlyExpenditure, record.negativeClientStatus,
            record.spousesFullName, record.spouseIdentificationType, record.spouseIdentificationNumber,
            record.maidenName, record.monthlyExpenses, record.birthDate, record.birthCountry,
            record.birthPostalCode, record.birthHouseNumber, record.birthRegion, record.birthDistrict,
            record.birthWard, record.birthStreet, record.identificationType, record.identificationNumber,
            record.issuanceDate, record.expirationDate, record.issuancePlace, record.issuingAuthority,
            record.businessName, record.establishmentDate, record.businessRegistrationNumber,
            record.businessRegistrationDate, record.businessLicenseNumber, record.taxIdentificationNumber,
            record.employerName, record.employerRegion, record.employerDistrict, record.employerWard,
            record.employerStreet, record.employerHouseNumber, record.employerPostalCode,
            record.businessNature, record.mobileNumber, record.alternativeMobileNumber,
            record.fixedLineNumber, record.faxNumber, record.emailAddress, record.socialMedia,
            record.mainAddress, record.street, record.houseNumber, record.postalCode, record.region,
            record.district, record.ward, record.country, record.sstreet, record.shouseNumber,
            record.spostalCode, record.sregion, record.sdistrict, record.sward, record.scountry
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for personal data"""
        return self.insert_to_postgres.__doc__  # Same as insert for now
    
    def validate_record(self, record: PersonalDataRecord) -> bool:
        """Validate Personal Data record"""
        if not super().validate_record(record):
            return False
        
        # Basic validations
        if not record.customerIdentificationNumber:
            return False
            
        return True