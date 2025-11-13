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

### Configuration File Approach (Recommended)

The easiest way to run the migration is using the configuration file approach:

1. **Copy the example configuration file:**
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` file with your configuration:**
   ```bash
   # Edit .env file with your values
   API_KEY=your_api_key_here
   SERVICE_NAME=privacera_snowflake
   SERVICE_TYPE=SNOWFLAKE
   TAG_TYPE=Internal
   BATCH_SIZE=100
   ```
   
   **Note:** The base URL `https://api.privaceracloud.com/api/` is hardcoded in the script. You only need to provide your `API_KEY` which will be appended to this base URL.

3. **Run the migration script:**
   ```bash
   ./run_migration.sh
   ```

The shell script will automatically read the `.env` file and execute the Python script with the appropriate parameters.

### Environment Variables

The `.env` file supports the following configuration options:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `API_KEY` | Yes | - | Your Privacera Cloud API key (appended to `https://api.privaceracloud.com/api/`) |
| `SERVICE_NAME` | Yes | - | Name of the service to migrate |
| `SERVICE_TYPE` | Yes | - | Service type (e.g., SNOWFLAKE, HIVE) |
| `TAG_TYPE` | No | `Internal` | Tag type (Sensitive, Confidential, Internal, Public) |
| `BATCH_SIZE` | No | `100` | Batch size for syncing (0 to disable batching) |