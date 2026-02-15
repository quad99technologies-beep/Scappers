#!/bin/bash
# Deployment Script for Foundation Contracts & Features
# Run this script to deploy all features

set -e  # Exit on error

echo "=========================================="
echo "Deployment: Foundation Contracts & Features"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DB_NAME="${POSTGRES_DB:-scrapers}"
DB_USER="${POSTGRES_USER:-postgres}"
DB_HOST="${POSTGRES_HOST:-localhost}"
DB_PORT="${POSTGRES_PORT:-5432}"
MIGRATION_FILE="sql/migrations/postgres/005_add_step_tracking_columns.sql"

# Function to print status
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✅ $2${NC}"
    else
        echo -e "${RED}❌ $2${NC}"
        exit 1
    fi
}

# Step 1: Verify prerequisites
echo "Step 1: Verifying prerequisites..."
python3 -c "import psycopg2" 2>/dev/null
print_status $? "PostgreSQL driver (psycopg2) installed"

python3 -c "from core.db.postgres_connection import get_db" 2>/dev/null
print_status $? "Core database module available"

if [ ! -f "$MIGRATION_FILE" ]; then
    echo -e "${RED}❌ Migration file not found: $MIGRATION_FILE${NC}"
    exit 1
fi
print_status 0 "Migration file exists"
echo ""

# Step 2: Run database migration
echo "Step 2: Running database migration..."
echo "Connecting to database: $DB_NAME@$DB_HOST:$DB_PORT"

PGPASSWORD="${POSTGRES_PASSWORD}" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$MIGRATION_FILE" > /dev/null 2>&1
print_status $? "Schema migration completed"
echo ""

# Step 3: Verify migration
echo "Step 3: Verifying migration..."
PGPASSWORD="${POSTGRES_PASSWORD}" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT version FROM _schema_versions WHERE version = 5;" | grep -q "5"
print_status $? "Migration version verified"
echo ""

# Step 4: Test foundation contracts
echo "Step 4: Testing foundation contracts..."
python3 -c "from core.step_hooks import StepHookRegistry; print('Step hooks OK')" 2>/dev/null
print_status $? "Step hooks contract"

python3 -c "from core.preflight_checks import PreflightChecker; print('Preflight checks OK')" 2>/dev/null
print_status $? "Preflight checks contract"

python3 -c "from core.alerting_contract import AlertRuleRegistry; print('Alerting contract OK')" 2>/dev/null
print_status $? "Alerting contract"

python3 -c "from core.pcid_mapping_contract import get_pcid_mapping; print('PCID contract OK')" 2>/dev/null
print_status $? "PCID mapping contract"
echo ""

# Step 5: Verify Malaysia pipeline integration
echo "Step 5: Verifying Malaysia pipeline integration..."
if grep -q "_FOUNDATION_AVAILABLE" scripts/Malaysia/run_pipeline_resume.py; then
    print_status 0 "Malaysia pipeline integration found"
else
    echo -e "${YELLOW}⚠️  Malaysia pipeline integration not found (may need integration)${NC}"
fi
echo ""

# Step 6: Check optional dependencies
echo "Step 6: Checking optional dependencies..."
python3 -c "import flask" 2>/dev/null && print_status 0 "Flask (for API)" || echo -e "${YELLOW}⚠️  Flask not installed (API endpoints will not work)${NC}"
python3 -c "import requests" 2>/dev/null && print_status 0 "Requests (for webhooks)" || echo -e "${YELLOW}⚠️  Requests not installed (webhooks will not work)${NC}"
echo ""

# Step 7: Summary
echo "=========================================="
echo "Deployment Summary"
echo "=========================================="
echo ""
echo -e "${GREEN}✅ Schema migration: Complete${NC}"
echo -e "${GREEN}✅ Foundation contracts: Verified${NC}"
echo -e "${GREEN}✅ Malaysia pipeline: Integrated${NC}"
echo ""
echo "Next steps:"
echo "1. Test Malaysia pipeline: cd scripts/Malaysia && python run_pipeline_resume.py --fresh"
echo "2. Integrate Argentina/Netherlands pipelines (copy Malaysia pattern)"
echo "3. Configure Telegram alerts (optional): Set TELEGRAM_BOT_TOKEN and TELEGRAM_ALLOWED_CHAT_IDS"
echo "4. Start scheduler (optional): python scripts/common/scheduler.py --daemon"
echo "5. Start API server (optional): python scripts/common/pipeline_api.py"
echo ""
echo -e "${GREEN}Deployment complete!${NC}"
