# Agents Pipeline - COMPLETED ✅

## Summary
Created a new **agents_streaming_pipeline.py** that uses the correct query from `sqls/agents-from-agents-list-NEW-V4.table.sql`.

## New Files Created
1. **db2-postgres-pipeline/agents_streaming_pipeline.py** - Main streaming pipeline
2. **db2-postgres-pipeline/run_agents_pipeline.py** - Runner script

## Old Files Removed
1. ~~simple_agents_pipeline.py~~ - Replaced by agents_streaming_pipeline.py
2. ~~simple_agent_check.py~~ - No longer needed
3. ~~create_agent_table.py~~ - Table should already exist

## Query Used (from AGENTS_LIST_V3)
```sql
SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
       al.AGENT_NAME                                     AS agentName,
       al.TERMINAL_ID                                    AS terminalId,
       al.AGENT_ID                                       AS agentId,
       al.TILL_NUMBER                                    AS tillNumber,
       al.BUSINESS_FORM                                  AS businessForm,
       al.AGENT_PRINCIPAL                                AS agentPrincipal,
       al.AGENT_PRINCIPAL_NAME                           AS agentPrincipalName,
       al.GENDER                                         AS gender,
       al.REGISTRATION_DATE                              AS registrationDate,
       al.CLOSED_DATE                                    AS closedDate,
       al.CERT_INCORPORATION                             AS certIncorporation,
       al.NATIONALITY                                    AS nationality,
       al.AGENT_STATUS                                   AS agentStatus,
       al.AGENT_TYPE                                     AS agentType,
       al.ACCOUNT_NUMBER                                 AS accountNumber,
       al.REGION                                         AS region,
       al.DISTRICT                                       AS district,
       al.WARD                                           AS ward,
       al.STREET                                         AS street,
       al.HOUSE_NUMBER                                   AS houseNumber,
       al.POSTAL_CODE                                    AS postalCode,
       al.COUNTRY                                        AS country,
       al.GPS_COORDINATES                                AS gpsCoordinates,
       al.AGENT_TAX_IDENTIFICATION_NUMBER                AS agentTaxIdentificationNumber,
       al.BUSINESS_LICENCE                               AS businessLicense
FROM AGENTS_LIST_V3 al;
```

## Pipeline Features
- ✅ Uses RabbitMQ for queuing
- ✅ Producer-Consumer pattern with threading
- ✅ Cursor-based pagination (by AGENT_ID)
- ✅ Retry logic for DB2 and PostgreSQL
- ✅ Progress tracking with statistics
- ✅ UPSERT support (ON CONFLICT DO UPDATE)
- ✅ Proper error handling and logging

## How to Run
```bash
# Run the agents streaming pipeline
python db2-postgres-pipeline\run_agents_pipeline.py

# Or with custom batch size
python db2-postgres-pipeline\agents_streaming_pipeline.py --batch-size 1000
```

## Note on config.py
The old complex query in `config.py` (lines 698-880) is still there but is NOT used by the new streaming pipeline. The new pipeline uses the query directly from AGENTS_LIST_V3 as shown above.
