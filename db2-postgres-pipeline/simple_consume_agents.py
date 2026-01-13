#!/usr/bin/env python3
"""
Simple consumer for remaining agents messages
"""

import pika
import json
import psycopg2
import logging
from config import Config

def simple_consume_agents():
    """Simple consumer to process remaining agents messages"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🔄 SIMPLE AGENTS CONSUMER")
    logger.info("=" * 50)
    
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    pg_cursor = pg_conn.cursor()
    
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Ensure queue exists
    channel.queue_declare(queue='agents_queue', durable=True)
    
    # Check initial queue status
    method = channel.queue_declare(queue='agents_queue', passive=True)
    initial_count = method.method.message_count
    logger.info(f"📊 Initial messages in queue: {initial_count}")
    
    processed_count = 0
    failed_count = 0
    
    # Upsert query
    upsert_query = """
    INSERT INTO agents (
        "reportingDate", "agentName", "agentId", "tillNumber", "businessForm",
        "agentPrincipal", "agentPrincipalName", "gender", "registrationDate", "closedDate",
        "certIncorporation", "nationality", "agentStatus", "agentType", "accountNumber",
        "region", "district", "ward", "street", "houseNumber",
        "postalCode", "country", "gpsCoordinates", "agentTaxIdentificationNumber", "businessLicense",
        "lastModified"
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s
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
        "lastModified" = EXCLUDED."lastModified",
        "updated_at" = CURRENT_TIMESTAMP
    """
    
    def callback(ch, method, properties, body):
        nonlocal processed_count, failed_count
        try:
            # Parse message
            agent_data = json.loads(body)
            
            # Extract data
            reporting_date = agent_data.get('reportingDate')
            agent_name = agent_data.get('agentName', '')
            agent_id = agent_data.get('agentId')
            till_number = agent_data.get('tillNumber')
            business_form = agent_data.get('businessForm', '')
            agent_principal = agent_data.get('agentPrincipal', '')
            agent_principal_name = agent_data.get('agentPrincipalName', '')
            gender = agent_data.get('gender', '')
            registration_date = agent_data.get('registrationDate')
            closed_date = agent_data.get('closedDate')
            cert_incorporation = agent_data.get('certIncorporation', '')
            nationality = agent_data.get('nationality', '')
            agent_status = agent_data.get('agentStatus', '')
            agent_type = agent_data.get('agentType', '')
            account_number = agent_data.get('accountNumber')
            region = agent_data.get('region', '')
            district = agent_data.get('district', '')
            ward = agent_data.get('ward', '')
            street = agent_data.get('street', '')
            house_number = agent_data.get('houseNumber', '')
            postal_code = agent_data.get('postalCode', '')
            country = agent_data.get('country', '')
            gps_coordinates = agent_data.get('gpsCoordinates')
            agent_tax_id = agent_data.get('agentTaxIdentificationNumber', '')
            business_license = agent_data.get('businessLicense', '')
            last_modified = agent_data.get('lastModified')
            
            # Insert into PostgreSQL
            pg_cursor.execute(upsert_query, (
                reporting_date, agent_name, agent_id, till_number, business_form,
                agent_principal, agent_principal_name, gender, registration_date, closed_date,
                cert_incorporation, nationality, agent_status, agent_type, account_number,
                region, district, ward, street, house_number,
                postal_code, country, gps_coordinates, agent_tax_id, business_license,
                last_modified
            ))
            
            processed_count += 1
            logger.info(f"✅ Processed agent: {agent_name} (ID: {agent_id})")
            
            # Commit every 10 records
            if processed_count % 10 == 0:
                pg_conn.commit()
                logger.info(f"📊 Progress: {processed_count} agents processed")
            
            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
                
        except Exception as e:
            failed_count += 1
            logger.error(f"❌ Error processing message: {e}")
            # Acknowledge to remove from queue (don't requeue to avoid infinite loop)
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    # Set up consumer
    channel.basic_qos(prefetch_count=1)  # Process one message at a time
    channel.basic_consume(queue='agents_queue', on_message_callback=callback)
    
    logger.info("🔄 Starting to consume remaining agents messages...")
    
    try:
        # Process messages until queue is empty
        while True:
            # Check if queue is empty
            method = channel.queue_declare(queue='agents_queue', passive=True)
            current_count = method.method.message_count
            
            if current_count == 0:
                logger.info("✅ All messages processed - queue is empty")
                break
            
            # Process messages for a short time
            connection.process_data_events(time_limit=2)
                
    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped by user")
    
    # Final commit
    pg_conn.commit()
    
    # Final status
    method = channel.queue_declare(queue='agents_queue', passive=True)
    final_count = method.method.message_count
    
    # Check PostgreSQL count
    pg_cursor.execute('SELECT COUNT(*) FROM agents')
    total_agents = pg_cursor.fetchone()[0]
    
    logger.info("=" * 50)
    logger.info(f"📊 CONSUMPTION SUMMARY:")
    logger.info(f"   - Initial messages: {initial_count}")
    logger.info(f"   - Processed messages: {processed_count}")
    logger.info(f"   - Failed messages: {failed_count}")
    logger.info(f"   - Remaining messages: {final_count}")
    logger.info(f"   - Total agents in PostgreSQL: {total_agents}")
    
    # Close connections
    pg_cursor.close()
    pg_conn.close()
    connection.close()
    
    logger.info("✅ Simple agents consumption completed!")

if __name__ == "__main__":
    simple_consume_agents()