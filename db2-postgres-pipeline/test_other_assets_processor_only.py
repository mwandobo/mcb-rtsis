#!/usr/bin/env python3
"""
Test just the Other Assets processor without full pipeline
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from processors.other_assets_processor import OtherAssetsProcessor
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_processor():
    """Test the processor with sample data"""
    
    # Sample data matching the query structure
    sample_data = (
        datetime.now(),  # reportingDate
        'Gold',          # assetType
        '2024-01-15',    # transactionDate
        '2024-12-31',    # maturityDate
        'Test Debtor',   # debtorName
        'Tanzania',      # debtorCountry
        'TZS',           # currency
        1000000.00,      # orgAmount
        None,            # usdAmount
        1000000.00,      # tzsAmount
        'Other Non-Financial Corporations',  # sectorSnaClassification
        0,               # pastDueDays
        1,               # assetClassificationCategory
        0.00,            # allowanceProbableLoss
        0.00             # botProvision
    )
    
    try:
        logger.info("Testing Other Assets processor...")
        processor = OtherAssetsProcessor()
        
        # Process the record
        record = processor.process_record(sample_data, 'other_assets')
        logger.info(f"Processed record: {record.asset_type}, {record.org_amount}, {record.currency}")
        
        # Validate the record
        is_valid = processor.validate_record(record)
        logger.info(f"Record validation: {is_valid}")
        
        # Test upsert query generation
        query = processor.get_upsert_query()
        logger.info(f"Generated query length: {len(query)} characters")
        
        logger.info("Processor test completed successfully!")
        
    except Exception as e:
        logger.error(f"Processor test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_processor()