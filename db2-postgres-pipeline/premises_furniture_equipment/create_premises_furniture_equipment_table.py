#!/usr/bin/env python3
"""Create or align the PostgreSQL target table for the premises-furniture-equipment pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from premises_furniture_equipment.premises_furniture_equipment_streaming_pipeline import PremisesFurnitureEquipmentStreamingPipeline


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    PremisesFurnitureEquipmentStreamingPipeline().setup_target_table()
    logging.getLogger(__name__).info('Premises-furniture-equipment target table is ready.')
