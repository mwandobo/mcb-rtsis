# Pipeline Monitoring Dashboard

Complete monitoring dashboard for your data pipelines.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Pipeline Dashboard                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────────────────┐     ┌─────────────────────┐          │
│   │   Frontend          │     │   Backend           │          │
│   │   (React + Vite)    │◄───►│   (NestJS + TypeORM)│          │
│   │   Port: 3001        │     │   Port: 3000        │          │
│   └─────────────────────┘     └─────────────────────┘          │
│            │                           │                       │
│            │                           │                       │
│            ▼                           ▼                       │
│   ┌─────────────────────────────────────────────────┐          │
│   │              PostgreSQL Database                 │          │
│   │              (pipeline_state table)              │          │
│   └─────────────────────────────────────────────────┘          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Start the Backend

```bash
cd db2-postgres-pipeline/dashboard

# Install dependencies
npm install

# Setup environment
cp .env.example .env
# Edit .env with your database credentials

# Start development server
npm run start:dev
```

Backend will run at `http://localhost:3000`
API docs at `http://localhost:3000/api/docs`

### 2. Start the Frontend

```bash
cd db2-postgres-pipeline/dashboard-frontend

# Install dependencies
npm install

# Start development server
npm run dev
```

Frontend will run at `http://localhost:3001`

## Features

### Dashboard Overview
- Total pipelines count
- Currently running pipelines
- Failed pipelines today
- Success rate percentage
- Records processed over time (chart)
- Pipeline runs over time (chart)

### Pipeline Management
- View all pipelines with status
- Run pipeline (incremental or full load)
- Stop running pipeline
- Clear pipeline queue
- View pipeline configuration

### Pipeline Details
- Current status and statistics
- Run history
- Configuration display
- Action controls

## API Endpoints

### Pipelines
```
GET    /api/pipelines              # List all pipelines
GET    /api/pipelines/:name        # Get pipeline details
GET    /api/pipelines/:name/history # Get run history
GET    /api/pipelines/:name/config # Get configuration
POST   /api/pipelines/:name/run    # Run pipeline
POST   /api/pipelines/:name/stop   # Stop pipeline
DELETE /api/pipelines/:name/queue  # Clear queue
GET    /api/pipelines/running/all  # Get running pipelines
```

### Statistics
```
GET /api/stats                     # Dashboard stats
GET /api/stats/records-over-time   # Chart data
GET /api/stats/top-pipelines       # Top pipelines
GET /api/stats/recent-failures     # Recent failures
GET /api/stats/health              # System health
```

## Environment Variables

### Backend (.env)
```env
DB_HOST=localhost
DB_PORT=5432
DB_USERNAME=postgres
DB_PASSWORD=postgres
DB_NAME=bankdb

RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest

PORT=3000
NODE_ENV=development
PIPELINES_DIR=../
```

## Production Build

### Backend
```bash
cd db2-postgres-pipeline/dashboard
npm run build
npm run start:prod
```

### Frontend
```bash
cd db2-postgres-pipeline/dashboard-frontend
npm run build
# Serve the dist/ folder with nginx or any static server
```

## Project Structure

```
db2-postgres-pipeline/
├── dashboard/                    # NestJS Backend
│   ├── src/
│   │   ├── pipelines/           # Pipeline management
│   │   ├── stats/               # Statistics endpoints
│   │   ├── config/              # Configuration
│   │   └── main.ts
│   ├── package.json
│   └── README.md
│
└── dashboard-frontend/          # React Frontend
    ├── src/
    │   ├── pages/               # Dashboard, PipelineDetail
    │   ├── components/          # Reusable components
    │   ├── services/            # API client
    │   └── types/               # TypeScript types
    ├── package.json
    └── README.md
```