# DB2 to PostgreSQL Real-Time Sync Pipeline

## Architecture
```
DB2 ──▶ Kafka Connect (JDBC Source) ──▶ Kafka ──▶ Kafka Connect (JDBC Sink) ──▶ PostgreSQL
```

## Prerequisites
1. Kafka Cluster (3 brokers recommended for HA)
2. Kafka Connect (distributed mode)
3. Schema Registry
4. DB2 JDBC Driver (db2jcc4.jar)
5. PostgreSQL JDBC Driver

## Directory Structure
```
kafka-connect/
├── config/
│   ├── connect-distributed.properties    # Kafka Connect config
│   └── connectors/
│       ├── source/                       # DB2 source connectors
│       └── sink/                         # PostgreSQL sink connectors
├── scripts/
│   ├── deploy-connectors.sh              # Deploy all connectors
│   └── monitor-connectors.sh             # Health check script
└── sql/
    └── postgres-schema.sql               # Target PostgreSQL schema
```

## Deployment Steps
1. Start Kafka cluster
2. Start Kafka Connect workers
3. Deploy source connectors: `./scripts/deploy-connectors.sh source`
4. Deploy sink connectors: `./scripts/deploy-connectors.sh sink`
5. Monitor: `./scripts/monitor-connectors.sh`

## Sync Latency
- Real-time connectors: 2-5 seconds
- Batch connectors (income-statement): Daily schedule
