# DB2 to PostgreSQL Real-Time Sync - Complete Setup Guide

## Prerequisites

Before starting, ensure you have:
- Docker Desktop installed and running
- DB2 database accessible from your machine
- Network access between Docker containers and DB2 server
- At least 8GB RAM available for Docker

---

## Step 1: Install Docker Desktop

### Windows:
1. Download from: https://www.docker.com/products/docker-desktop
2. Run installer and follow prompts
3. Restart computer if prompted
4. Open Docker Desktop and wait for it to start

### Verify installation:
```cmd
docker --version
docker-compose --version
```

---

## Step 2: Copy DB2 JDBC Driver

Open Command Prompt as Administrator:

```cmd
cd path\to\your\project

mkdir kafka-connect\jdbc-drivers

copy "C:\Program Files\IBM\SQLLIB\java\db2jcc4.jar" kafka-connect\jdbc-drivers\
copy "C:\Program Files\IBM\SQLLIB\java\db2jcc_license_cu.jar" kafka-connect\jdbc-drivers\
```

Verify files exist:
```cmd
dir kafka-connect\jdbc-drivers
```


---

## Step 3: Configure Environment Variables

1. Copy the template:
```cmd
copy kafka-connect\scripts\env.template kafka-connect\.env
```

2. Edit `kafka-connect\.env` with your actual values:
```properties
# DB2 Source Connection (YOUR ACTUAL VALUES)
DB2_HOST=192.168.1.100          # Your DB2 server IP
DB2_PORT=50000                   # DB2 port (default 50000)
DB2_DATABASE=BANKDB              # Your database name
DB2_USER=db2admin                # DB2 username
DB2_PASSWORD=YourSecurePassword  # DB2 password

# PostgreSQL Target Connection
PG_HOST=postgres                 # Keep as 'postgres' for Docker
PG_PORT=5432
PG_DATABASE=bankdb
PG_USER=postgres
PG_PASSWORD=postgres

# Kafka Configuration
KAFKA_BOOTSTRAP=kafka1:29092
KAFKA_CONNECT_URL=http://localhost:8083
```

---

## Step 4: Update Connector Configurations

Replace placeholder variables in all source connector JSON files.

Edit each file in `kafka-connect/config/connectors/source/` and replace:
- `${DB2_HOST}` → your actual DB2 host (e.g., `192.168.1.100`)
- `${DB2_PORT}` → your DB2 port (e.g., `50000`)
- `${DB2_DATABASE}` → your database name (e.g., `BANKDB`)
- `${DB2_USER}` → your DB2 username
- `${DB2_PASSWORD}` → your DB2 password

Example - Edit `cash-information-source.json`:
```json
"connection.url": "jdbc:db2://192.168.1.100:50000/BANKDB",
"connection.user": "db2admin",
"connection.password": "YourSecurePassword",
```

Do this for ALL 14 source connector files.

---

## Step 5: Update Sink Connector

Edit `kafka-connect/config/connectors/sink/postgres-sink.json`:
```json
"connection.url": "jdbc:postgresql://postgres:5432/bankdb",
"connection.user": "postgres",
"connection.password": "postgres",
```


---

## Step 6: Start the Infrastructure

Open terminal in the `kafka-connect` folder:

```cmd
cd kafka-connect

docker-compose up -d
```

This starts:
- Zookeeper (coordination)
- Kafka broker (message queue)
- Kafka Connect (sync engine)
- PostgreSQL (target database)
- Kafka UI (monitoring dashboard)

Wait 2-3 minutes for all services to start.

### Verify all containers are running:
```cmd
docker-compose ps
```

Expected output - all should show "Up":
```
NAME            STATUS
zookeeper       Up
kafka1          Up
kafka-connect   Up
postgres        Up
kafka-ui        Up
```

### Check Kafka Connect is ready:
```cmd
curl http://localhost:8083/
```

Should return: `{"version":"...","commit":"...","kafka_cluster_id":"..."}`

---

## Step 7: Verify PostgreSQL Schema

The schema is auto-created, but verify:

```cmd
docker exec -it postgres psql -U postgres -d bankdb -c "\dt"
```

Should list all tables (cash_information, loan_information, etc.)

---

## Step 8: Deploy Source Connectors

### Option A: Using the script (Linux/Mac/Git Bash):
```bash
chmod +x scripts/deploy-connectors.sh
./scripts/deploy-connectors.sh source
```

### Option B: Manual deployment (Windows CMD):

Deploy each connector using curl:

```cmd
curl -X POST -H "Content-Type: application/json" ^
  -d @config/connectors/source/cash-information-source.json ^
  http://localhost:8083/connectors

curl -X POST -H "Content-Type: application/json" ^
  -d @config/connectors/source/loan-information-source.json ^
  http://localhost:8083/connectors

curl -X POST -H "Content-Type: application/json" ^
  -d @config/connectors/source/loan-transaction-source.json ^
  http://localhost:8083/connectors
```

Repeat for all source connectors.


---

## Step 9: Deploy Sink Connector

```cmd
curl -X POST -H "Content-Type: application/json" ^
  -d @config/connectors/sink/postgres-sink.json ^
  http://localhost:8083/connectors
```

---

## Step 10: Verify Connectors are Running

### Check all connectors:
```cmd
curl http://localhost:8083/connectors
```

### Check specific connector status:
```cmd
curl http://localhost:8083/connectors/db2-cash-information-source/status
```

Should show: `"state": "RUNNING"`

### Using Kafka UI (Recommended):
Open browser: http://localhost:8080
- Click "Kafka Connect" in sidebar
- See all connectors and their status

---

## Step 11: Verify Data is Syncing

### Check PostgreSQL for data:
```cmd
docker exec -it postgres psql -U postgres -d bankdb

-- Inside psql:
SELECT COUNT(*) FROM cash_information;
SELECT * FROM cash_information LIMIT 5;
\q
```

### Check Kafka topics have messages:
Open http://localhost:8080 → Topics → Select any `bank.db2.*` topic

---

## Troubleshooting

### Problem: Connector shows FAILED

Check the error:
```cmd
curl http://localhost:8083/connectors/db2-cash-information-source/status
```

Common issues:
- **Connection refused**: DB2 host not reachable from Docker
- **Authentication failed**: Wrong username/password
- **Table not found**: Check your SQL query

### Problem: Cannot connect to DB2 from Docker

Add your DB2 host to docker-compose.yml under kafka-connect:
```yaml
extra_hosts:
  - "db2server:192.168.1.100"
```

Or use host network mode:
```yaml
network_mode: "host"
```

### Problem: No data in PostgreSQL

1. Check source connector is RUNNING
2. Check Kafka topics have messages (Kafka UI)
3. Check sink connector is RUNNING
4. Check sink connector errors

### View Kafka Connect logs:
```cmd
docker logs kafka-connect --tail 100
```


---

## Step 12: Production Checklist

Before going to production:

- [ ] Change PostgreSQL password in docker-compose.yml
- [ ] Use secrets management for DB credentials (not plain text)
- [ ] Set up 3 Kafka brokers for high availability
- [ ] Configure TLS/SSL for all connections
- [ ] Set up monitoring alerts
- [ ] Test failover scenarios
- [ ] Document recovery procedures

---

## Quick Reference Commands

```cmd
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# View logs
docker logs kafka-connect -f

# Restart Kafka Connect
docker-compose restart kafka-connect

# List all connectors
curl http://localhost:8083/connectors

# Pause a connector
curl -X PUT http://localhost:8083/connectors/db2-cash-information-source/pause

# Resume a connector
curl -X PUT http://localhost:8083/connectors/db2-cash-information-source/resume

# Delete a connector
curl -X DELETE http://localhost:8083/connectors/db2-cash-information-source

# Check connector status
curl http://localhost:8083/connectors/db2-cash-information-source/status
```

---

## URLs

| Service | URL |
|---------|-----|
| Kafka Connect REST API | http://localhost:8083 |
| Kafka UI Dashboard | http://localhost:8080 |
| PostgreSQL | localhost:5432 |

---

## Support

If you encounter issues:
1. Check Docker logs: `docker logs <container-name>`
2. Verify network connectivity to DB2
3. Ensure all credentials are correct
4. Check Kafka UI for connector errors
