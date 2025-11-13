#!/bin/bash

# Script to run Ranger to Omni Metadata migration using configuration from .env file

set -e  # Exit on error

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Error: .env file not found in $SCRIPT_DIR"
    echo "Please copy .env.example to .env and update with your configuration"
    exit 1
fi

# Hardcoded base URL for Privacera Cloud API
BASE_ADMIN_URL="https://api.privaceracloud.com/api/"

# Source the .env file
# This will load all the environment variables
set -a  # Automatically export all variables
source .env
set +a  # Stop automatically exporting

# Validate required variables
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

# Construct the full admin URL by appending API_KEY to the base URL
ADMIN_URL="${BASE_ADMIN_URL}${API_KEY}"

# Set defaults for optional variables
TAG_TYPE=${TAG_TYPE:-Internal}
BATCH_SIZE=${BATCH_SIZE:-100}
SYNC_TAGS_TO_MDS=${SYNC_TAGS_TO_MDS:-true}
SYNC_RESOURCES_TO_MDS=${SYNC_RESOURCES_TO_MDS:-true}
SYNC_TAG_RESOURCE_MAPPINGS=${SYNC_TAG_RESOURCE_MAPPINGS:-true}
INCLUDE_TAGS=${INCLUDE_TAGS:-false}

# Build the Python command arguments as an array (safer for special characters)
PYTHON_ARGS=(
    "ranger_tag_to_omni_migrate.py"
    "--admin-url" "$ADMIN_URL"
    "--service-name" "$SERVICE_NAME"
    "--service-type" "$SERVICE_TYPE"
    "--tag-type" "$TAG_TYPE"
    "--batch-size" "$BATCH_SIZE"
)

# Add optional flags
if [ "$SYNC_TAGS_TO_MDS" = "true" ]; then
    PYTHON_ARGS+=("--sync-tags-to-mds")
fi

if [ "$SYNC_RESOURCES_TO_MDS" = "true" ]; then
    PYTHON_ARGS+=("--sync-resources-to-mds")
fi

if [ "$SYNC_TAG_RESOURCE_MAPPINGS" = "true" ]; then
    PYTHON_ARGS+=("--sync-tag-resource-mappings")
fi

if [ "$INCLUDE_TAGS" = "true" ]; then
    PYTHON_ARGS+=("--include-tags")
fi

if [ -n "$SAVE_TAGS_JSON" ] && [ "$SAVE_TAGS_JSON" != "" ]; then
    PYTHON_ARGS+=("--save-tags-json" "$SAVE_TAGS_JSON")
fi

# Print configuration summary
echo "=========================================="
echo "Ranger to Omni Metadata Migration"
echo "=========================================="
echo "Admin URL: ${BASE_ADMIN_URL}${API_KEY:0:10}..." # Show only first 10 chars of API key for security
echo "Service Name: $SERVICE_NAME"
echo "Service Type: $SERVICE_TYPE"
echo "Tag Type: $TAG_TYPE"
echo "Batch Size: $BATCH_SIZE"
echo "Sync Tags: $SYNC_TAGS_TO_MDS"
echo "Sync Resources: $SYNC_RESOURCES_TO_MDS"
echo "Sync Mappings: $SYNC_TAG_RESOURCE_MAPPINGS"
echo "Include Tags: $INCLUDE_TAGS"
if [ -n "$SAVE_TAGS_JSON" ] && [ "$SAVE_TAGS_JSON" != "" ]; then
    echo "Save Tags JSON: $SAVE_TAGS_JSON"
fi
echo "=========================================="
echo ""

# Execute the Python script
python3 "${PYTHON_ARGS[@]}"

# Capture exit code
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "Migration completed successfully!"
else
    echo ""
    echo "Migration failed with exit code: $EXIT_CODE"
fi

exit $EXIT_CODE


