"""
Employee information processor - Based on employee.sql
"""

from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from .base import BaseProcessor, BaseRecord

@dataclass
class EmployeeRecord(BaseRecord):
    """Employee information record structure - Based on employee.sql"""
    reporting_date: str
    branch_code: str
    emp_name: str
    gender: str
    emp_dob: str
    emp_identification_type: str
    emp_identification_number: Optional[str]
    emp_position: str
    emp_position_category: str
    emp_status: str
    emp_department: str
    appointment_date: str
    emp_nationality: str
    last_promotion_date: str
    basic_salary: Optional[str]
    emp_benefits: Optional[str]
    retry_count: int = 0

class EmployeeProcessor(BaseProcessor):
    """Processor for employee information data - Based on employee.sql"""
    
    def process_record(self, raw_data: Tuple, table_name: str) -> EmployeeRecord:
        """Convert raw DB2 data to EmployeeRecord - Updated for employee.sql"""
        # raw_data structure from employee.sql (16 fields):
        # 0: reportingDate, 1: branchCode, 2: empName, 3: gender, 4: empDob,
        # 5: empIdentificationType, 6: empIdentificationNumber, 7: empPosition, 8: empPositionCategory,
        # 9: empStatus, 10: empDepartment, 11: appointmentDate, 12: empNationality,
        # 13: lastPromotionDate, 14: basicSalary, 15: empBenefits
        return EmployeeRecord(
            source_table=table_name,
            timestamp_column_value=str(raw_data[11]),  # appointmentDate for tracking
            reporting_date=str(raw_data[0]),
            branch_code=str(raw_data[1]) if raw_data[1] else None,
            emp_name=str(raw_data[2]).strip(),
            gender=str(raw_data[3]),
            emp_dob=str(raw_data[4]),
            emp_identification_type=str(raw_data[5]),
            emp_identification_number=str(raw_data[6]) if raw_data[6] else None,
            emp_position=str(raw_data[7]),
            emp_position_category=str(raw_data[8]),
            emp_status=str(raw_data[9]),
            emp_department=str(raw_data[10]),
            appointment_date=str(raw_data[11]),
            emp_nationality=str(raw_data[12]),
            last_promotion_date=str(raw_data[13]),
            basic_salary=str(raw_data[14]) if raw_data[14] else None,
            emp_benefits=str(raw_data[15]) if raw_data[15] else None,
            original_timestamp=datetime.now().isoformat()
        )
    
    def insert_to_postgres(self, record: EmployeeRecord, pg_cursor) -> None:
        """Insert employee record to PostgreSQL"""
        query = self.get_upsert_query()
        
        pg_cursor.execute(query, (
            record.reporting_date,
            record.branch_code,
            record.emp_name,
            record.gender,
            record.emp_dob,
            record.emp_identification_type,
            record.emp_identification_number,
            record.emp_position,
            record.emp_position_category,
            record.emp_status,
            record.emp_department,
            record.appointment_date,
            record.emp_nationality,
            record.last_promotion_date,
            record.basic_salary,
            record.emp_benefits
        ))
    
    def get_upsert_query(self) -> str:
        """Get upsert query for employee information with duplicate handling"""
        return """
        INSERT INTO "employeeInformation" (
            "reportingDate", "branchCode", "empName", "gender", "empDob",
            "empIdentificationType", "empIdentificationNumber", "empPosition", "empPositionCategory",
            "empStatus", "empDepartment", "appointmentDate", "empNationality",
            "lastPromotionDate", "basicSalary", "empBenefits"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("empName", "empIdentificationNumber") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "branchCode" = EXCLUDED."branchCode",
            "gender" = EXCLUDED."gender",
            "empDob" = EXCLUDED."empDob",
            "empIdentificationType" = EXCLUDED."empIdentificationType",
            "empPosition" = EXCLUDED."empPosition",
            "empPositionCategory" = EXCLUDED."empPositionCategory",
            "empStatus" = EXCLUDED."empStatus",
            "empDepartment" = EXCLUDED."empDepartment",
            "appointmentDate" = EXCLUDED."appointmentDate",
            "empNationality" = EXCLUDED."empNationality",
            "lastPromotionDate" = EXCLUDED."lastPromotionDate",
            "basicSalary" = EXCLUDED."basicSalary",
            "empBenefits" = EXCLUDED."empBenefits"
        """
    
    def validate_record(self, record: EmployeeRecord) -> bool:
        """Validate employee record"""
        if not super().validate_record(record):
            return False
        
        # Employee-specific validations
        if not record.emp_name or record.emp_name.strip() == '':
            return False
        if not record.gender or record.gender.strip() == '':
            return False
        if not record.emp_position or record.emp_position.strip() == '':
            return False
        if not record.emp_department or record.emp_department.strip() == '':
            return False
            
        return True
    
    def transform_data(self, raw_data: Tuple) -> dict:
        """Transform employee data"""
        # Add any employee-specific transformations here
        return {
            'emp_name_normalized': str(raw_data[2]).strip().upper(),
            'position_normalized': str(raw_data[7]).strip().upper(),
            'department_normalized': str(raw_data[10]).strip().upper()
        }