# Streaming Pipelines Summary

## Newly Created Pipelines

### 1. Loan Transaction Streaming Pipeline ✅
**Files:**
- `loan_transaction_streaming_pipeline.py` - Main pipeline
- `run_loan_transaction_pipeline.py` - Runner script
- `create_loan_transactions_table.py` - Table creation script

**Source:** `sqls/loan-transaction.sql`
**Table:** `loanTransaction` (camelCase)
**Queue:** `loan_transaction_queue`
**Batch Size:** 1000 records

**Run:**
```bash
python db2-postgres-pipeline\run_loan_transaction_pipeline.py
```

---

### 2. Agents Streaming Pipeline ✅
**Files:**
- `agents_streaming_pipeline.py` - Main pipeline
- `run_agents_pipeline.py` - Runner script

**Source:** `sqls/agents-from-agents-list-NEW-V4.table.sql`
**Table:** `agents` (uses AGENTS_LIST_V3)
**Queue:** `agents_queue`
**Batch Size:** 500 records

**Run:**
```bash
python db2-postgres-pipeline\run_agents_pipeline.py
```

**Removed Old Files:**
- ~~simple_agents_pipeline.py~~
- ~~simple_agent_check.py~~
- ~~create_agent_table.py~~

---

## Pipeline Architecture

Both pipelines follow the same proven pattern:

### Components
1. **Producer Thread** - Reads from DB2 and publishes to RabbitMQ
2. **Consumer Thread** - Consumes from RabbitMQ and writes to PostgreSQL
3. **RabbitMQ Queue** - Message broker for reliable processing
4. **Cursor-based Pagination** - Efficient batch processing

### Features
- ✅ Concurrent producer-consumer processing
- ✅ Retry logic for DB2, RabbitMQ, and PostgreSQL
- ✅ Progress tracking with statistics
- ✅ Error handling and logging
- ✅ Configurable batch sizes
- ✅ UPSERT support (ON CONFLICT)

### Flow
```
DB2 → Producer → RabbitMQ Queue → Consumer → PostgreSQL
     (batches)              (messages)        (records)
```

---

## Common Commands

### Run with default settings
```bash
python db2-postgres-pipeline\run_loan_transaction_pipeline.py
python db2-postgres-pipeline\run_agents_pipeline.py
```

### Run with custom batch size
```bash
python db2-postgres-pipeline\loan_transaction_streaming_pipeline.py --batch-size 2000
python db2-postgres-pipeline\agents_streaming_pipeline.py --batch-size 1000
```

### Monitor RabbitMQ queues
```bash
# Check queue status
rabbitmqctl list_queues name messages consumers

# Purge a queue if needed
rabbitmqctl purge_queue loan_transaction_queue
rabbitmqctl purge_queue agents_queue
```

---

## Performance Tips

1. **Batch Size**
   - Larger batches (1000-2000): Faster for high-performance systems
   - Smaller batches (500-1000): Better for limited resources

2. **RabbitMQ**
   - Ensure RabbitMQ is running before starting pipelines
   - Monitor queue depth to ensure consumer keeps up

3. **Database Connections**
   - Pipelines use connection pooling
   - Retry logic handles temporary connection issues

4. **Progress Monitoring**
   - Logs show batch progress every batch
   - Detailed progress reports every 5 minutes
   - Final summary at completion

---

## Troubleshooting

### Pipeline hangs
- Check DB2 connection
- Check RabbitMQ is running
- Check PostgreSQL connection

### Slow processing
- Increase batch size
- Check network latency
- Monitor DB2 query performance

### Messages stuck in queue
- Check consumer thread logs
- Verify PostgreSQL table exists
- Check for data validation errors

---

## Next Steps

To create more streaming pipelines, follow this pattern:
1. Create `{name}_streaming_pipeline.py` with Producer/Consumer
2. Create `run_{name}_pipeline.py` runner script
3. Use cursor-based pagination for efficient batching
4. Include retry logic and progress tracking
5. Remove any old/duplicate pipeline files
