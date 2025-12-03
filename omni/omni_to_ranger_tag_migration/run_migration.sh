#!/bin/bash

# Script to run Omni to Ranger tag migration using configuration from .env file

set -e  # Exit on error

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Error: .env file not found in $SCRIPT_DIR"
    echo "Please create a .env file with the following variables:"
    echo "  API_KEY=your_api_key_here"
    echo "  BASE_URL=https://api.privaceracloud.com/api  # Optional, defaults to https://api.privaceracloud.com/api"
    echo "  SERVICE_NAME=your_service_name"
    echo "  SERVICE_TYPE=your_service_type"
    echo "  RANGER_USERNAME=your_ranger_username  # Optional, for Ranger basic auth"
    echo "  RANGER_PASSWORD=your_ranger_password  # Optional, for Ranger basic auth"
    echo ""
    echo "Note: BASE_URL defaults to https://api.privaceracloud.com/api if not specified."
    exit 1
fi

# Source the .env file
# This will load all the environment variables
set -a  # Automatically export all variables
source .env
set +a  # Stop automatically exporting

# Check if retry-failed-only mode is enabled
RETRY_FAILED_ONLY=${RETRY_FAILED_ONLY:-false}

# Validate required variables (skip service name/type if retry-failed-only is enabled)
if [ "$RETRY_FAILED_ONLY" != "true" ]; then
    if [ -z "$API_KEY" ]; then
        echo "Error: API_KEY is not set in .env file"
        exit 1
    fi

    if [ -z "$SERVICE_NAME" ]; then
        echo "Error: SERVICE_NAME is not set in .env file"
        exit 1
    fi

    if [ -z "$SERVICE_TYPE" ]; then
        echo "Error: SERVICE_TYPE is not set in .env file"
        exit 1
    fi
fi

# Set defaults for optional variables
SAVE_PAYLOAD=${SAVE_PAYLOAD:-""}

# Build the Python command arguments as an array (safer for special characters)
PYTHON_ARGS=(
    "omni_tag_to_ranger_migrate.py"
)

# If retry-failed-only is enabled, skip service name/type requirements
if [ "$RETRY_FAILED_ONLY" = "true" ]; then
    PYTHON_ARGS+=("--retry-failed-only")
    # Only add API key if provided (base URL is hardcoded in script)
    if [ -n "$API_KEY" ]; then
        PYTHON_ARGS+=("--api-key" "$API_KEY")
    fi
else
    # Normal migration requires service name and type
    PYTHON_ARGS+=("--api-key" "$API_KEY")
    PYTHON_ARGS+=("--service-name" "$SERVICE_NAME")
    PYTHON_ARGS+=("--service-type" "$SERVICE_TYPE")
fi

# Add BASE_URL if provided
if [ -n "$BASE_URL" ] && [ "$BASE_URL" != "" ]; then
    PYTHON_ARGS+=("--base-url" "$BASE_URL")
fi

# Add optional arguments
if [ -n "$SAVE_PAYLOAD" ] && [ "$SAVE_PAYLOAD" != "" ]; then
    PYTHON_ARGS+=("--save-payload" "$SAVE_PAYLOAD")
fi

# Add Ranger authentication if provided
if [ -n "$RANGER_USERNAME" ] && [ -n "$RANGER_PASSWORD" ]; then
    PYTHON_ARGS+=("--ranger-username" "$RANGER_USERNAME")
    PYTHON_ARGS+=("--ranger-password" "$RANGER_PASSWORD")
fi

# Print configuration summary
echo "=========================================="
echo "Omni to Ranger Tag Migration"
echo "=========================================="
if [ "$RETRY_FAILED_ONLY" = "true" ]; then
    echo "Mode: Retry Failed Batches Only"
    if [ -n "$API_KEY" ]; then
        echo "API Key: ${API_KEY:0:10}..." # Show only first 10 chars of API key for security
    fi
    if [ -n "$BASE_URL" ] && [ "$BASE_URL" != "" ]; then
        echo "Base URL: $BASE_URL (from .env)"
    else
        echo "Base URL: https://api.privaceracloud.com/api (default)"
    fi
else
    echo "Mode: Full Migration"
    if [ -n "$BASE_URL" ] && [ "$BASE_URL" != "" ]; then
        echo "Base URL: $BASE_URL (from .env)"
    else
        echo "Base URL: https://api.privaceracloud.com/api (default)"
    fi
    echo "API Key: ${API_KEY:0:10}..." # Show only first 10 chars of API key for security
    echo "Service Name: $SERVICE_NAME"
    echo "Service Type: $SERVICE_TYPE"
fi
if [ -n "$SAVE_PAYLOAD" ] && [ "$SAVE_PAYLOAD" != "" ]; then
    echo "Save Payload: $SAVE_PAYLOAD"
fi
echo "Retry Failed Only: $RETRY_FAILED_ONLY"
if [ -n "$RANGER_USERNAME" ] && [ -n "$RANGER_PASSWORD" ]; then
    echo "Ranger Auth: Enabled (username: $RANGER_USERNAME)"
fi
echo "=========================================="
echo ""

# Execute the Python script
# Temporarily disable set -e to capture exit code
set +e
python3 "${PYTHON_ARGS[@]}"
EXIT_CODE=$?
set -e

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "Migration completed successfully!"
else
    echo ""
    echo "Migration failed with exit code: $EXIT_CODE"
fi

exit $EXIT_CODE

