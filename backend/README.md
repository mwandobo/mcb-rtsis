# Pipeline Monitoring Dashboard API

NestJS backend for monitoring and managing data pipelines.

## Prerequisites

- Node.js 18+
- PostgreSQL database with `pipeline_state` table
- npm or yarn

## Installation

```bash
# Install dependencies
npm install

# Copy environment file
cp .env.example .env

# Edit .env with your database credentials
```

## Configuration

Edit `.env` file:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USERNAME=postgres
DB_PASSWORD=postgres
DB_NAME=bankdb

# RabbitMQ Configuration
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest

# App Configuration
PORT=3000
NODE_ENV=development

# Pipeline Directory (relative path to pipeline root)
PIPELINES_DIR=../
```

## Running the Application

```bash
# Development mode with hot reload
npm run start:dev

# Production mode
npm run build
npm run start:prod
```

## API Endpoints

### Pipelines

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/pipelines` | Get all pipelines with status |
| GET | `/api/pipelines/:name` | Get specific pipeline details |
| GET | `/api/pipelines/:name/history` | Get pipeline run history |
| GET | `/api/pipelines/:name/config` | Get pipeline configuration |
| POST | `/api/pipelines/:name/run` | Trigger pipeline run |
| POST | `/api/pipelines/:name/stop` | Stop running pipeline |
| PUT | `/api/pipelines/:name/config` | Update pipeline config |
| DELETE | `/api/pipelines/:name/queue` | Clear pipeline queue |
| GET | `/api/pipelines/running/all` | Get running pipelines |

### Statistics

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/stats` | Get dashboard summary stats |
| GET | `/api/stats/records-over-time` | Get records over time |
| GET | `/api/stats/top-pipelines` | Get top pipelines |
| GET | `/api/stats/recent-failures` | Get recent failures |
| GET | `/api/stats/health` | Get system health status |

## Example Usage

```bash
# Get all pipelines
curl http://localhost:3000/api/pipelines

# Run a pipeline
curl -X POST http://localhost:3000/api/pipelines/cash/run

# Run with full load
curl -X POST http://localhost:3000/api/pipelines/cash/run \
  -H "Content-Type: application/json" \
  -d '{"fullLoad": true}'

# Get dashboard stats
curl http://localhost:3000/api/stats
```

## API Documentation

Swagger documentation available at: `http://localhost:3000/api/docs`

## Project Structure

```
src/
├── main.ts                    # Application entry point
├── app.module.ts              # Root module
├── config/
│   ├── config.module.ts       # Config module
│   └── config.service.ts      # Config service
├── pipelines/
│   ├── pipelines.module.ts    # Pipelines module
│   ├── pipelines.controller.ts
│   ├── pipelines.service.ts
│   └── entities/
│       └── pipeline-state.entity.ts
└── stats/
    ├── stats.module.ts        # Stats module
    ├── stats.controller.ts
    └── stats.service.ts
```