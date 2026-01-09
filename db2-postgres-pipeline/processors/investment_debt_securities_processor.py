#!/usr/bin/env python3
"""
Investment Debt Securities processor for RTSIS reporting
"""

import logging
from typing import Dict, Any, Optional
from decimal import Decimal
from datetime import datetime, date

class InvestmentDebtSecuritiesProcessor:
    """Processor for investment debt securities data"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def process_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single investment debt securities record
        
        Args:
            record: Raw record from DB2 (with lowercase column names)
            
        Returns:
            Processed record ready for PostgreSQL insertion
        """
        try:
            processed = {}
            
            # Timestamp fields
            processed['reportingDate'] = self._process_timestamp(record.get('reportingdate'))
            processed['originalTimestamp'] = datetime.now()
            
            # Security identification
            processed['securityNumber'] = self._clean_string(record.get('securitynumber'))
            processed['securityType'] = self._clean_string(record.get('securitytype'))
            processed['securityIssuerName'] = self._clean_string(record.get('securityissuername'))
            
            # Rating information
            processed['externalIssuerRatting'] = self._clean_string(record.get('externalissuerratting'))
            processed['gradesUnratedBanks'] = self._clean_string(record.get('gradesunratedbanks'))
            
            # Geographic and sector information
            processed['securityIssuerCountry'] = self._clean_string(record.get('securityissuercountry'))
            processed['snaIssuerSector'] = self._clean_string(record.get('snaissuersector'))
            
            # Currency
            processed['currency'] = self._clean_string(record.get('currency'))
            
            # Cost value amounts
            processed['orgCostValueAmount'] = self._process_decimal(record.get('orgcostvalueamount'))
            processed['tzsCostValueAmount'] = self._process_decimal(record.get('tzscostvalueamount'))
            processed['usdCostValueAmount'] = self._process_decimal(record.get('usdcostvalueamount'))
            
            # Face value amounts
            processed['orgFaceValueAmount'] = self._process_decimal(record.get('orgfacevalueamount'))
            processed['tzsgFaceValueAmount'] = self._process_decimal(record.get('tzsgfacevalueamount'))
            processed['usdgFaceValueAmount'] = self._process_decimal(record.get('usdgfacevalueamount'))
            
            # Fair value amounts
            processed['orgFairValueAmount'] = self._process_decimal(record.get('orgfairvalueamount'))
            processed['tzsgFairValueAmount'] = self._process_decimal(record.get('tzsgfairvalueamount'))
            processed['usdgFairValueAmount'] = self._process_decimal(record.get('usdgfairvalueamount'))
            
            # Interest rate
            processed['interestRate'] = self._process_decimal(record.get('interestrate'), precision=6)
            
            # Date fields
            processed['purchaseDate'] = self._process_date(record.get('purchasedate'))
            processed['valueDate'] = self._process_date(record.get('valuedate'))
            processed['maturityDate'] = self._process_date(record.get('maturitydate'))
            
            # Trading and encumbrance information
            processed['tradingIntent'] = self._clean_string(record.get('tradingintent'))
            processed['securityEncumbaranceStatus'] = self._clean_string(record.get('securityencumbarancestatus'))
            
            # Risk and classification
            processed['pastDueDays'] = self._process_integer(record.get('pastduedays'))
            processed['allowanceProbableLoss'] = self._process_decimal(record.get('allowanceprobableloss'))
            processed['assetClassificationCategory'] = self._process_integer(record.get('assetclassificationcategory'))
            
            return processed
            
        except Exception as e:
            self.logger.error(f"Error processing investment debt securities record: {e}")
            self.logger.error(f"Record data: {record}")
            raise
    
    def _clean_string(self, value: Any) -> Optional[str]:
        """Clean and validate string values"""
        if value is None:
            return None
        
        # Convert to string and strip whitespace
        cleaned = str(value).strip()
        
        # Return None for empty strings
        if not cleaned:
            return None
            
        return cleaned
    
    def _process_decimal(self, value: Any, precision: int = 2) -> Optional[Decimal]:
        """Process decimal values with proper precision"""
        if value is None:
            return None
        
        try:
            # Handle string values
            if isinstance(value, str):
                value = value.strip()
                if not value or value.lower() in ('null', 'none', ''):
                    return None
            
            # Convert to Decimal with specified precision
            decimal_value = Decimal(str(value))
            return decimal_value.quantize(Decimal('0.' + '0' * precision))
            
        except (ValueError, TypeError, Exception) as e:
            self.logger.warning(f"Could not convert '{value}' to decimal: {e}")
            return None
    
    def _process_integer(self, value: Any) -> Optional[int]:
        """Process integer values"""
        if value is None:
            return None
        
        try:
            # Handle string values
            if isinstance(value, str):
                value = value.strip()
                if not value or value.lower() in ('null', 'none', ''):
                    return None
            
            return int(float(value))  # Convert via float to handle decimal strings
            
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Could not convert '{value}' to integer: {e}")
            return None
    
    def _process_date(self, value: Any) -> Optional[date]:
        """Process date values"""
        if value is None:
            return None
        
        try:
            if isinstance(value, date):
                return value
            elif isinstance(value, datetime):
                return value.date()
            elif isinstance(value, str):
                value = value.strip()
                if not value or value.lower() in ('null', 'none', ''):
                    return None
                # Try parsing common date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d %H:%M:%S']:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
                        
        except Exception as e:
            self.logger.warning(f"Could not convert '{value}' to date: {e}")
        
        return None
    
    def _process_timestamp(self, value: Any) -> Optional[datetime]:
        """Process timestamp values"""
        if value is None:
            return datetime.now()
        
        try:
            if isinstance(value, datetime):
                return value
            elif isinstance(value, date):
                return datetime.combine(value, datetime.min.time())
            elif isinstance(value, str):
                value = value.strip()
                if not value or value.lower() in ('null', 'none', ''):
                    return datetime.now()
                # Try parsing common timestamp formats
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S', '%d/%m/%Y']:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                        
        except Exception as e:
            self.logger.warning(f"Could not convert '{value}' to timestamp: {e}")
        
        return datetime.now()
    
    def get_upsert_query(self) -> str:
        """Get the upsert query for investment debt securities"""
        return '''
        INSERT INTO "investmentDebtSecurities" (
            "reportingDate", "securityNumber", "securityType", "securityIssuerName",
            "externalIssuerRatting", "gradesUnratedBanks", "securityIssuerCountry", "snaIssuerSector",
            "currency", "orgCostValueAmount", "tzsCostValueAmount", "usdCostValueAmount",
            "orgFaceValueAmount", "tzsgFaceValueAmount", "usdgFaceValueAmount",
            "orgFairValueAmount", "tzsgFairValueAmount", "usdgFairValueAmount",
            "interestRate", "purchaseDate", "valueDate", "maturityDate",
            "tradingIntent", "securityEncumbaranceStatus", "pastDueDays",
            "allowanceProbableLoss", "assetClassificationCategory", "originalTimestamp"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT ("securityNumber") 
        DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "securityType" = EXCLUDED."securityType",
            "securityIssuerName" = EXCLUDED."securityIssuerName",
            "externalIssuerRatting" = EXCLUDED."externalIssuerRatting",
            "gradesUnratedBanks" = EXCLUDED."gradesUnratedBanks",
            "securityIssuerCountry" = EXCLUDED."securityIssuerCountry",
            "snaIssuerSector" = EXCLUDED."snaIssuerSector",
            "currency" = EXCLUDED."currency",
            "orgCostValueAmount" = EXCLUDED."orgCostValueAmount",
            "tzsCostValueAmount" = EXCLUDED."tzsCostValueAmount",
            "usdCostValueAmount" = EXCLUDED."usdCostValueAmount",
            "orgFaceValueAmount" = EXCLUDED."orgFaceValueAmount",
            "tzsgFaceValueAmount" = EXCLUDED."tzsgFaceValueAmount",
            "usdgFaceValueAmount" = EXCLUDED."usdgFaceValueAmount",
            "orgFairValueAmount" = EXCLUDED."orgFairValueAmount",
            "tzsgFairValueAmount" = EXCLUDED."tzsgFairValueAmount",
            "usdgFairValueAmount" = EXCLUDED."usdgFairValueAmount",
            "interestRate" = EXCLUDED."interestRate",
            "purchaseDate" = EXCLUDED."purchaseDate",
            "valueDate" = EXCLUDED."valueDate",
            "maturityDate" = EXCLUDED."maturityDate",
            "tradingIntent" = EXCLUDED."tradingIntent",
            "securityEncumbaranceStatus" = EXCLUDED."securityEncumbaranceStatus",
            "pastDueDays" = EXCLUDED."pastDueDays",
            "allowanceProbableLoss" = EXCLUDED."allowanceProbableLoss",
            "assetClassificationCategory" = EXCLUDED."assetClassificationCategory",
            "originalTimestamp" = EXCLUDED."originalTimestamp"
        '''
    
    def get_insert_params(self, processed_record: Dict[str, Any]) -> tuple:
        """Get parameters for the insert query"""
        return (
            processed_record.get('reportingDate'),
            processed_record.get('securityNumber'),
            processed_record.get('securityType'),
            processed_record.get('securityIssuerName'),
            processed_record.get('externalIssuerRatting'),
            processed_record.get('gradesUnratedBanks'),
            processed_record.get('securityIssuerCountry'),
            processed_record.get('snaIssuerSector'),
            processed_record.get('currency'),
            processed_record.get('orgCostValueAmount'),
            processed_record.get('tzsCostValueAmount'),
            processed_record.get('usdCostValueAmount'),
            processed_record.get('orgFaceValueAmount'),
            processed_record.get('tzsgFaceValueAmount'),
            processed_record.get('usdgFaceValueAmount'),
            processed_record.get('orgFairValueAmount'),
            processed_record.get('tzsgFairValueAmount'),
            processed_record.get('usdgFairValueAmount'),
            processed_record.get('interestRate'),
            processed_record.get('purchaseDate'),
            processed_record.get('valueDate'),
            processed_record.get('maturityDate'),
            processed_record.get('tradingIntent'),
            processed_record.get('securityEncumbaranceStatus'),
            processed_record.get('pastDueDays'),
            processed_record.get('allowanceProbableLoss'),
            processed_record.get('assetClassificationCategory'),
            processed_record.get('originalTimestamp')
        )