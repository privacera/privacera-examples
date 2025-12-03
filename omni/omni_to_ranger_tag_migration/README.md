# Omni to Ranger Tag Migration Script

A Python script to download tag definitions and tag-resource mappings from Omni Metadata Service and migrate them to Apache Ranger. The script converts Omni data to Ranger ServiceTags format and pushes it to Ranger Admin API.

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
pip install -r requirements.txt
```

## Usage

### Configuration File Approach (Recommended)

The easiest way to run the migration is using the configuration file approach:

1. **Copy `.env.example` to `.env` and configure it:**
   ```bash
   cp .env.example .env
   ```
   
   Then edit `.env` with your configuration:
   ```bash
   API_KEY=your_api_key_here
   SERVICE_NAME=privacera_snowflake
   SERVICE_TYPE=SNOWFLAKE
   RANGER_USERNAME=admin
   RANGER_PASSWORD=admin
   ```
   
   **Note:** The base URL `https://api.privaceracloud.com/api/` is hardcoded in the script. You only need to provide your `API_KEY` which will be appended to this base URL.

2. **Run the migration script:**
   ```bash
   ./run_migration.sh
   ```

The shell script will automatically read the `.env` file and execute the Python script with the appropriate parameters.

### Environment Variables

The `.env` file supports the following configuration options:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `API_KEY` | Yes | - | API key for authentication (used for both Omni and Ranger APIs). The base URL `https://api.privaceracloud.com/api/` is hardcoded in the script. |
| `SERVICE_NAME` | Yes | - | Name of the service in both Omni and Ranger |
| `SERVICE_TYPE` | Yes | - | Service type (e.g., SNOWFLAKE, HIVE, HDFS) |
| `RANGER_USERNAME` | No | - | Username for Ranger API basic authentication |
| `RANGER_PASSWORD` | No | - | Password for Ranger API basic authentication |
| `LOG_LEVEL` | No | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| `RETRY_FAILED_ONLY` | No | `false` | Set to `true` to skip main migration and only retry failed batches from `failed_batches/` directory |

## Retry Failed Batches Mode

When `RETRY_FAILED_ONLY=true`, the script skips the main migration and only retries batches that failed in a previous run. The script processes all `failed_batch_*.json` files in the `failed_batches/` directory.

**Behavior:**
- Skips fetching tags and mappings from Omni
- Retries each failed batch (up to 5 attempts)
- Successfully retried files are renamed to `*_retried_successfully.json`
- `SERVICE_NAME` and `SERVICE_TYPE` are not required (stored in failed batch files)

**Example:**
```bash
# .env file
RETRY_FAILED_ONLY=true
API_KEY=your_api_key_here
```

**Note:** The base URL `https://api.privaceracloud.com/api/` is hardcoded in the script. You only need to provide your `API_KEY` which will be appended to this base URL.

```bash
./run_migration.sh
```
