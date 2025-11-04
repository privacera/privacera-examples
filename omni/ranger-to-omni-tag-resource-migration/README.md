# Ranger Tag to Omni Metadata Migration Script

A Python script to download tags and tagged resources from Ranger Admin API and migrate them to Omni Metadata Service (MDS). The script supports syncing tag definitions, resources, and tag-resource mappings to MDS.

## Installation

### 1. Create and Activate Virtual Environment (Recommended)

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate

# On Windows:
# venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install "requests>=2.31.0"
```

## Usage

## Supported Tag Types

The script validates that `--tag-type` is one of the following supported values:

- **Sensitive**
- **Confidential**
- **Internal** (default)
- **Public**

## Getting the App Code

The `--app-code` parameter can be found in the connector configuration file:

**Location**: `privacera/privacera-manager/output/kubernetes/helm/connector/<connector-name>/<instance-name>/conf/policysync.properties`

**Key**: `policysync.appCode`

### Basic Usage

Complete example with all supported parameters:

```bash
python3 ranger_tag_to_omni_migrate.py \
  --admin-url https://api.privaceracloud.com/api/3dXXXXXXXXXX498cd5433795c9c5c0dd16 \
  --service-name privacera_snowflake \
  --app-code APP001 \
  --service-type SNOWFLAKE \
  --sync-tags-to-mds \
  --sync-resources-to-mds \
  --sync-tag-resource-mappings \
  --tag-type Sensitive \
  --batch-size 100
```
