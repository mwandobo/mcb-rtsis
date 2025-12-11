#!/bin/bash
# Monitor Kafka Connect connectors health
# Usage: ./monitor-connectors.sh [--watch]

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo "=============================================="
    echo "  Kafka Connect Status - $(date)"
    echo "=============================================="
    echo ""
    
    # Get all connectors
    connectors=$(curl -s "$CONNECT_URL/connectors" | jq -r '.[]')
    
    if [ -z "$connectors" ]; then
        echo "No connectors found"
        return
    fi
    
    printf "%-40s %-12s %-12s\n" "CONNECTOR" "STATE" "TASKS"
    printf "%-40s %-12s %-12s\n" "----------------------------------------" "------------" "------------"
    
    for connector in $connectors; do
        status=$(curl -s "$CONNECT_URL/connectors/$connector/status")
        
        state=$(echo "$status" | jq -r '.connector.state')
        task_count=$(echo "$status" | jq -r '.tasks | length')
        task_states=$(echo "$status" | jq -r '.tasks[].state' | sort | uniq -c | tr '\n' ' ')
        
        # Color based on state
        case $state in
            RUNNING)
                state_color="${GREEN}${state}${NC}"
                ;;
            PAUSED)
                state_color="${YELLOW}${state}${NC}"
                ;;
            FAILED)
                state_color="${RED}${state}${NC}"
                ;;
            *)
                state_color="$state"
                ;;
        esac
        
        printf "%-40s ${state_color}%-12s${NC} %s\n" "$connector" "" "$task_states"
    done
    
    echo ""
    echo "=============================================="
}

# Check for failed connectors and show errors
show_errors() {
    echo ""
    echo "Checking for errors..."
    
    connectors=$(curl -s "$CONNECT_URL/connectors" | jq -r '.[]')
    
    for connector in $connectors; do
        status=$(curl -s "$CONNECT_URL/connectors/$connector/status")
        state=$(echo "$status" | jq -r '.connector.state')
        
        if [ "$state" == "FAILED" ]; then
            echo ""
            echo -e "${RED}FAILED: $connector${NC}"
            echo "$status" | jq -r '.connector.trace' | head -20
        fi
        
        # Check task failures
        failed_tasks=$(echo "$status" | jq -r '.tasks[] | select(.state == "FAILED")')
        if [ -n "$failed_tasks" ]; then
            echo ""
            echo -e "${RED}FAILED TASKS in $connector:${NC}"
            echo "$status" | jq -r '.tasks[] | select(.state == "FAILED") | .trace' | head -20
        fi
    done
}

# Lag monitoring (requires consumer group access)
show_lag() {
    echo ""
    echo "Consumer Lag (if available):"
    # This requires kafka-consumer-groups.sh to be available
    if command -v kafka-consumer-groups.sh &> /dev/null; then
        kafka-consumer-groups.sh --bootstrap-server ${KAFKA_BOOTSTRAP:-localhost:9092} \
            --group connect-bank-db2-pg-connect-cluster \
            --describe 2>/dev/null | head -20
    else
        echo "kafka-consumer-groups.sh not found in PATH"
    fi
}

# Main
if [ "$1" == "--watch" ]; then
    while true; do
        clear
        print_status
        show_errors
        sleep 10
    done
else
    print_status
    show_errors
fi
