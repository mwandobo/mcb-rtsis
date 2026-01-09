"""
Personal data information processor - Based on personal_data_information.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class PersonalDataRecord(BaseRecord):
    """Personal data information record structure"""
    reporting_date: str
    customer_identification_number: str
    first_name: Optional[str]
    middle_names: Optional[str]
    other_names: Optional[str]
    full_names: Optional[str]
    present_surname: Optional[str]
    birth_surname: Optional[str]
    gender: Optional[str]
    marital_status: Optional[str]
    number_spouse: Optional[str]
    spouses_full_name: Optional[str]
    nationality: Optional[str]
    citizenship: Optional[str]
    residency: Optional[str]
    profession: Optional[str]
    sector_sna_classification: Optional[str]
    fate_status: Optional[str]
    social_status: Optional[str]
    employment_status: Optional[str]
    monthly_income: Optional[str]
    number_dependants: Optional[str]
    education_level: Optional[str]
    average_monthly_expenditure: Optional[str]
    monthly_expenses: Optional[str]
    negative_client_status: Optional[str]
    spouse_identification_type: Optional[str]
    spouse_identification_number: Optional[str]
    maiden_name: Optional[str]
    birth_date: Optional[str]
    birth_country: Optional[str]
    birth_postal_code: Optional[str]
    birth_house_number: Optional[str]
    birth_region: Optional[str]
    birth_district: Optional[str]
    birth_ward: Optional[str]
    birth_street: Optional[str]
    identification_type: Optional[str]
    identification_number: Optional[str]
    issuance_date: Optional[str]
    expiration_date: Optional[str]
    issuance_place: Optional[str]
    issuing_authority: Optional[str]
    business_name: Optional[str]
    establishment_date: Optional[str]
    business_registration_number: Optional[str]
    business_registration_date: Optional[str]
    business_license_number: Optional[str]
    tax_identification_number: Optional[str]
    employer_name: Optional[str]
    employer_region: Optional[str]
    employer_district: Optional[str]
    employer_ward: Optional[str]
    employer_street: Optional[str]
    employer_house_number: Optional[str]
    employer_postal_code: Optional[str]
    business_nature: Optional[str]
    mobile_number: Optional[str]
    alternative_mobile_number: Optional[str]
    fixed_line_number: Optional[str]
    fax_number: Optional[str]
    email_address: Optional[str]
    social_media: Optional[str]
    main_address: Optional[str]
    street: Optional[str]
    house_number: Optional[str]
    postal_code: Optional[str]
    region: Optional[str]
    district: Optional[str]
    ward: Optional[str]
    country: Optional[str]
    work_street: Optional[str]
    work_house_number: Optional[str]
    work_postal_code: Optional[str]
    work_region: Optional[str]
    work_district: Optional[str]
    work_ward: Optional[str]
    work_country: Optional[str]
    retry_count: int = 0

class PersonalDataProcessor(BaseProcessor):
    """Processor for personal data information - Based on personal_data_information.sql"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> PersonalDataRecord:
        """Convert raw DB2 data to PersonalDataRecord"""
        return PersonalDataRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[0]),  # reportingDate for tracking
            reporting_date=str(raw_data[0]),
            customer_identification_number=str(raw_data[1]),
            first_name=str(raw_data[2]).strip() if raw_data[2] else None,
            middle_names=str(raw_data[3]).strip() if raw_data[3] else None,
            other_names=str(raw_data[4]).strip() if raw_data[4] else None,
            full_names=str(raw_data[5]).strip() if raw_data[5] else None,
            present_surname=str(raw_data[6]).strip() if raw_data[6] else None,
            birth_surname=str(raw_data[7]).strip() if raw_data[7] else None,
            gender=str(raw_data[8]).strip() if raw_data[8] else None,
            marital_status=str(raw_data[9]).strip() if raw_data[9] else None,
            number_spouse=str(raw_data[10]).strip() if raw_data[10] else None,
            spouses_full_name=str(raw_data[11]).strip() if raw_data[11] else None,
            nationality=str(raw_data[12]).strip() if raw_data[12] else None,
            citizenship=str(raw_data[13]).strip() if raw_data[13] else None,
            residency=str(raw_data[14]).strip() if raw_data[14] else None,
            profession=str(raw_data[15]).strip() if raw_data[15] else None,
            sector_sna_classification=str(raw_data[16]).strip() if raw_data[16] else None,
            fate_status=str(raw_data[17]).strip() if raw_data[17] else None,
            social_status=str(raw_data[18]).strip() if raw_data[18] else None,
            employment_status=str(raw_data[19]).strip() if raw_data[19] else None,
            monthly_income=str(raw_data[20]).strip() if raw_data[20] else None,
            number_dependants=str(raw_data[21]).strip() if raw_data[21] else None,
            education_level=str(raw_data[22]).strip() if raw_data[22] else None,
            average_monthly_expenditure=str(raw_data[23]).strip() if raw_data[23] else None,
            monthly_expenses=str(raw_data[24]).strip() if raw_data[24] else None,
            negative_client_status=str(raw_data[25]).strip() if raw_data[25] else None,
            spouse_identification_type=str(raw_data[26]).strip() if raw_data[26] else None,
            spouse_identification_number=str(raw_data[27]).strip() if raw_data[27] else None,
            maiden_name=str(raw_data[28]).strip() if raw_data[28] else None,
            birth_date=str(raw_data[29]) if raw_data[29] else None,
            birth_country=str(raw_data[30]).strip() if raw_data[30] else None,
            birth_postal_code=str(raw_data[31]).strip() if raw_data[31] else None,
            birth_house_number=str(raw_data[32]).strip() if raw_data[32] else None,
            birth_region=str(raw_data[33]).strip() if raw_data[33] else None,
            birth_district=str(raw_data[34]).strip() if raw_data[34] else None,
            birth_ward=str(raw_data[35]).strip() if raw_data[35] else None,
            birth_street=str(raw_data[36]).strip() if raw_data[36] else None,
            identification_type=str(raw_data[37]).strip() if raw_data[37] else None,
            identification_number=str(raw_data[38]).strip() if raw_data[38] else None,
            issuance_date=str(raw_data[39]) if raw_data[39] else None,
            expiration_date=str(raw_data[40]) if raw_data[40] else None,
            issuance_place=str(raw_data[41]).strip() if raw_data[41] else None,
            issuing_authority=str(raw_data[42]).strip() if raw_data[42] else None,
            business_name=str(raw_data[43]).strip() if raw_data[43] else None,
            establishment_date=str(raw_data[44]) if raw_data[44] else None,
            business_registration_number=str(raw_data[45]).strip() if raw_data[45] else None,
            business_registration_date=str(raw_data[46]) if raw_data[46] else None,
            business_license_number=str(raw_data[47]).strip() if raw_data[47] else None,
            tax_identification_number=str(raw_data[48]).strip() if raw_data[48] else None,
            employer_name=str(raw_data[49]).strip() if raw_data[49] else None,
            employer_region=str(raw_data[50]).strip() if raw_data[50] else None,
            employer_district=str(raw_data[51]).strip() if raw_data[51] else None,
            employer_ward=str(raw_data[52]).strip() if raw_data[52] else None,
            employer_street=str(raw_data[53]).strip() if raw_data[53] else None,
            employer_house_number=str(raw_data[54]).strip() if raw_data[54] else None,
            employer_postal_code=str(raw_data[55]).strip() if raw_data[55] else None,
            business_nature=str(raw_data[56]).strip() if raw_data[56] else None,
            mobile_number=str(raw_data[57]).strip() if raw_data[57] else None,
            alternative_mobile_number=str(raw_data[58]).strip() if raw_data[58] else None,
            fixed_line_number=str(raw_data[59]).strip() if raw_data[59] else None,
            fax_number=str(raw_data[60]).strip() if raw_data[60] else None,
            email_address=str(raw_data[61]).strip() if raw_data[61] else None,
            social_media=str(raw_data[62]).strip() if raw_data[62] else None,
            main_address=str(raw_data[63]).strip() if raw_data[63] else None,
            street=str(raw_data[64]).strip() if raw_data[64] else None,
            house_number=str(raw_data[65]).strip() if raw_data[65] else None,
            postal_code=str(raw_data[66]).strip() if raw_data[66] else None,
            region=str(raw_data[67]).strip() if raw_data[67] else None,
            district=str(raw_data[68]).strip() if raw_data[68] else None,
            ward=str(raw_data[69]).strip() if raw_data[69] else None,
            country=str(raw_data[70]).strip() if raw_data[70] else None,
            work_street=str(raw_data[71]).strip() if raw_data[71] else None,
            work_house_number=str(raw_data[72]).strip() if raw_data[72] else None,
            work_postal_code=str(raw_data[73]).strip() if raw_data[73] else None,
            work_region=str(raw_data[74]).strip() if raw_data[74] else None,
            work_district=str(raw_data[75]).strip() if raw_data[75] else None,
            work_ward=str(raw_data[76]).strip() if raw_data[76] else None,
            work_country=str(raw_data[77]).strip() if raw_data[77] else None,
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: PersonalDataRecord, pg_cursor) -> None:
        """Insert personal data record to PostgreSQL"""
        query = self.get_upsert_query()
        
        # Fixed: Added missing original_timestamp parameter to match 78 placeholders in query
        pg_cursor.execute(query, (
            record.reporting_date,
            record.customer_identification_number,
            record.first_name,
            record.middle_names,
            record.other_names,
            record.full_names,
            record.present_surname,
            record.birth_surname,
            record.gender,
            record.marital_status,
            record.number_spouse,
            record.spouses_full_name,
            record.nationality,
            record.citizenship,
            record.residency,
            record.profession,
            record.sector_sna_classification,
            record.fate_status,
            record.social_status,
            record.employment_status,
            record.monthly_income,
            record.number_dependants,
            record.education_level,
            record.average_monthly_expenditure,
            record.monthly_expenses,
            record.negative_client_status,
            record.spouse_identification_type,
            record.spouse_identification_number,
            record.maiden_name,
            record.birth_date,
            record.birth_country,
            record.birth_postal_code,
            record.birth_house_number,
            record.birth_region,
            record.birth_district,
            record.birth_ward,
            record.birth_street,
            record.identification_type,
            record.identification_number,
            record.issuance_date,
            record.expiration_date,
            record.issuance_place,
            record.issuing_authority,
            record.business_name,
            record.establishment_date,
            record.business_registration_number,
            record.business_registration_date,
            record.business_license_number,
            record.tax_identification_number,
            record.employer_name,
            record.employer_region,
            record.employer_district,
            record.employer_ward,
            record.employer_street,
            record.employer_house_number,
            record.employer_postal_code,
            record.business_nature,
            record.mobile_number,
            record.alternative_mobile_number,
            record.fixed_line_number,
            record.fax_number,
            record.email_address,
            record.social_media,
            record.main_address,
            record.street,
            record.house_number,
            record.postal_code,
            record.region,
            record.district,
            record.ward,
            record.country,
            record.work_street,
            record.work_house_number,
            record.work_postal_code,
            record.work_region,
            record.work_district,
            record.work_ward,
            record.work_country,
            record.original_timestamp  # Added missing parameter
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for personal data information with duplicate handling"""
        return """
        INSERT INTO "personalDataInformation" (
            "reportingDate", "customerIdentificationNumber", "firstName", "middleNames", "otherNames",
            "fullNames", "presentSurname", "birthSurname", "gender", "maritalStatus", "numberSpouse",
            "spousesFullName", "nationality", "citizenship", "residency", "profession", "sectorSnaClassification",
            "fateStatus", "socialStatus", "employmentStatus", "monthlyIncome", "numberDependants",
            "educationLevel", "averageMonthlyExpenditure", "monthlyExpenses", "negativeClientStatus",
            "spouseIdentificationType", "spouseIdentificationNumber", "maidenName", "birthDate",
            "birthCountry", "birthPostalCode", "birthHouseNumber", "birthRegion", "birthDistrict",
            "birthWard", "birthStreet", "identificationType", "identificationNumber", "issuanceDate",
            "expirationDate", "issuancePlace", "issuingAuthority", "businessName", "establishmentDate",
            "businessRegistrationNumber", "businessRegistrationDate", "businessLicenseNumber",
            "taxIdentificationNumber", "employerName", "employerRegion", "employerDistrict",
            "employerWard", "employerStreet", "employerHouseNumber", "employerPostalCode",
            "businessNature", "mobileNumber", "alternativeMobileNumber", "fixedLineNumber",
            "faxNumber", "emailAddress", "socialMedia", "mainAddress", "street", "houseNumber",
            "postalCode", "region", "district", "ward", "country", "workStreet", "workHouseNumber",
            "workPostalCode", "workRegion", "workDistrict", "workWard", "workCountry", "originalTimestamp"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("customerIdentificationNumber") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "firstName" = EXCLUDED."firstName",
            "middleNames" = EXCLUDED."middleNames",
            "otherNames" = EXCLUDED."otherNames",
            "fullNames" = EXCLUDED."fullNames",
            "presentSurname" = EXCLUDED."presentSurname",
            "birthSurname" = EXCLUDED."birthSurname",
            "gender" = EXCLUDED."gender",
            "maritalStatus" = EXCLUDED."maritalStatus",
            "numberSpouse" = EXCLUDED."numberSpouse",
            "spousesFullName" = EXCLUDED."spousesFullName",
            "nationality" = EXCLUDED."nationality",
            "citizenship" = EXCLUDED."citizenship",
            "residency" = EXCLUDED."residency",
            "profession" = EXCLUDED."profession",
            "sectorSnaClassification" = EXCLUDED."sectorSnaClassification",
            "fateStatus" = EXCLUDED."fateStatus",
            "socialStatus" = EXCLUDED."socialStatus",
            "employmentStatus" = EXCLUDED."employmentStatus",
            "monthlyIncome" = EXCLUDED."monthlyIncome",
            "numberDependants" = EXCLUDED."numberDependants",
            "educationLevel" = EXCLUDED."educationLevel",
            "averageMonthlyExpenditure" = EXCLUDED."averageMonthlyExpenditure",
            "monthlyExpenses" = EXCLUDED."monthlyExpenses",
            "negativeClientStatus" = EXCLUDED."negativeClientStatus",
            "spouseIdentificationType" = EXCLUDED."spouseIdentificationType",
            "spouseIdentificationNumber" = EXCLUDED."spouseIdentificationNumber",
            "maidenName" = EXCLUDED."maidenName",
            "birthDate" = EXCLUDED."birthDate",
            "birthCountry" = EXCLUDED."birthCountry",
            "birthPostalCode" = EXCLUDED."birthPostalCode",
            "birthHouseNumber" = EXCLUDED."birthHouseNumber",
            "birthRegion" = EXCLUDED."birthRegion",
            "birthDistrict" = EXCLUDED."birthDistrict",
            "birthWard" = EXCLUDED."birthWard",
            "birthStreet" = EXCLUDED."birthStreet",
            "identificationType" = EXCLUDED."identificationType",
            "identificationNumber" = EXCLUDED."identificationNumber",
            "issuanceDate" = EXCLUDED."issuanceDate",
            "expirationDate" = EXCLUDED."expirationDate",
            "issuancePlace" = EXCLUDED."issuancePlace",
            "issuingAuthority" = EXCLUDED."issuingAuthority",
            "businessName" = EXCLUDED."businessName",
            "establishmentDate" = EXCLUDED."establishmentDate",
            "businessRegistrationNumber" = EXCLUDED."businessRegistrationNumber",
            "businessRegistrationDate" = EXCLUDED."businessRegistrationDate",
            "businessLicenseNumber" = EXCLUDED."businessLicenseNumber",
            "taxIdentificationNumber" = EXCLUDED."taxIdentificationNumber",
            "employerName" = EXCLUDED."employerName",
            "employerRegion" = EXCLUDED."employerRegion",
            "employerDistrict" = EXCLUDED."employerDistrict",
            "employerWard" = EXCLUDED."employerWard",
            "employerStreet" = EXCLUDED."employerStreet",
            "employerHouseNumber" = EXCLUDED."employerHouseNumber",
            "employerPostalCode" = EXCLUDED."employerPostalCode",
            "businessNature" = EXCLUDED."businessNature",
            "mobileNumber" = EXCLUDED."mobileNumber",
            "alternativeMobileNumber" = EXCLUDED."alternativeMobileNumber",
            "fixedLineNumber" = EXCLUDED."fixedLineNumber",
            "faxNumber" = EXCLUDED."faxNumber",
            "emailAddress" = EXCLUDED."emailAddress",
            "socialMedia" = EXCLUDED."socialMedia",
            "mainAddress" = EXCLUDED."mainAddress",
            "street" = EXCLUDED."street",
            "houseNumber" = EXCLUDED."houseNumber",
            "postalCode" = EXCLUDED."postalCode",
            "region" = EXCLUDED."region",
            "district" = EXCLUDED."district",
            "ward" = EXCLUDED."ward",
            "country" = EXCLUDED."country",
            "workStreet" = EXCLUDED."workStreet",
            "workHouseNumber" = EXCLUDED."workHouseNumber",
            "workPostalCode" = EXCLUDED."workPostalCode",
            "workRegion" = EXCLUDED."workRegion",
            "workDistrict" = EXCLUDED."workDistrict",
            "workWard" = EXCLUDED."workWard",
            "workCountry" = EXCLUDED."workCountry",
            "originalTimestamp" = EXCLUDED."originalTimestamp"
        """
    
    def validate_record(self, record: PersonalDataRecord) -> bool:
        """Validate personal data record"""
        if not super().validate_record(record):
            return False
        
        # Personal data-specific validations
        if not record.customer_identification_number:
            return False
        if not record.reporting_date:
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform personal data"""
        # Add any personal data-specific transformations here
        return {
            'full_name_normalized': str(raw_data[5]).strip().upper() if raw_data[5] else None,
            'gender_normalized': str(raw_data[8]).strip().upper() if raw_data[8] else None,
            'mobile_masked': str(raw_data[57])[:3] + '****' + str(raw_data[57])[-3:] if raw_data[57] and len(str(raw_data[57])) >= 6 else str(raw_data[57])
        }