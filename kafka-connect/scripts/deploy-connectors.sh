#!/bin/bash
# Deploy Kafka Connect connectors for DB2 to PostgreSQL sync
# Usage: ./deploy-connectors.sh [source|sink|all]

set -e

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="$SCRIPT_DIR/../config/connectors"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check if Kafka Connect is available
check_connect() {
    log_info "Checking Kafka Connect availability at $CONNECT_URL..."
    if ! curl -s "$CONNECT_URL/" > /dev/null 2>&1; then
        log_error "Kafka Connect is not available at $CONNECT_URL"
        exit 1
    fi
    log_info "Kafka Connect is available"
}

# Deploy a single connector
deploy_connector() {
    local config_file=$1
    local connector_name=$(jq -r '.name' "$config_file")
    
    log_info "Deploying connector: $connector_name"
    
    # Check if connector exists
    local status_code=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors/$connector_name")
    
    if [ "$status_code" == "200" ]; then
        log_warn "Connector $connector_name exists, updating..."
        curl -s -X PUT \
            -H "Content-Type: application/json" \
            -d @"$config_file" \
            "$CONNECT_URL/connectors/$connector_name/config" | jq .
    else
        log_info "Creating new connector: $connector_name"
        curl -s -X POST \
            -H "Content-Type: application/json" \
            -d @"$config_file" \
            "$CONNECT_URL/connectors" | jq .
    fi
    
    # Verify deployment
    sleep 2
    local state=$(curl -s "$CONNECT_URL/connectors/$connector_name/status" | jq -r '.connector.state')
    if [ "$state" == "RUNNING" ]; then
        log_info "Connector $connector_name is RUNNING"
    else
        log_warn "Connector $connector_name state: $state"
    fi
}

# Deploy all connectors in a directory
deploy_directory() {
    local dir=$1
    log_info "Deploying connectors from: $dir"
    
    for config_file in "$dir"/*.json; do
        if [ -f "$config_file" ]; then
            deploy_connector "$config_file"
            echo ""
        fi
    done
}

# Main
check_connect

case "${1:-all}" in
    source)
        deploy_directory "$CONFIG_DIR/source"
        ;;
    sink)
        deploy_directory "$CONFIG_DIR/sink"
        ;;
    all)
        log_info "Deploying all connectors..."
        deploy_directory "$CONFIG_DIR/source"
        deploy_directory "$CONFIG_DIR/sink"
        ;;
    *)
        echo "Usage: $0 [source|sink|all]"
        exit 1
        ;;
esac

log_info "Deployment complete!"
