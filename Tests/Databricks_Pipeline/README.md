# Databricks Pipeline Test

This test validates the Databricks integration by creating a data pipeline that reads JSON data, writes to a Delta table, and registers it in the catalog.

## Environment Variables

### Required
- `DATABRICKS_HOST`: Your Databricks workspace hostname (e.g., `dbc-abc123-def.cloud.databricks.com`)
- `DATABRICKS_TOKEN`: Your Databricks personal access token

### Optional
- `DATABRICKS_CLUSTER_ID`: Existing cluster ID to use (if not provided, a new test cluster will be created)
- `DATABRICKS_HTTP_PATH`: SQL warehouse HTTP path (defaults to `/sql/1.0/warehouses/default`)

## Getting Your Databricks Credentials

### 1. Databricks Host
Your workspace URL without the `https://` prefix. For example:
- Full URL: `https://dbc-abc123-def.cloud.databricks.com`
- Host value: `dbc-abc123-def.cloud.databricks.com`

### 2. Personal Access Token
1. Go to your Databricks workspace
2. Click your username in the top right → User Settings
3. Go to the Access tokens tab
4. Click "Generate new token"
5. Give it a name like "DE-Bench Testing"
6. Set expiration (90 days recommended)
7. Copy the generated token

### 3. Cluster ID (Optional)
If you want to use an existing cluster:
1. Go to Compute in your Databricks workspace
2. Click on your cluster
3. Copy the Cluster ID from the URL or cluster details

## Example .env file

```bash
DATABRICKS_HOST=dbc-abc123-def.cloud.databricks.com
DATABRICKS_TOKEN=dapi1234567890abcdef
DATABRICKS_CLUSTER_ID=0613-213020-example
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/52b55fb76db6ead4
```

## Test Behavior

### Safe Configuration
The test uses safe, non-production settings:
- **Catalog**: `hive_metastore` (default catalog)
- **Schema**: `default` (default schema)
- **Table**: `transactions` (test table name)
- **Data Paths**: `/tmp/` and `/pipelines/raw/` (test directories)

### What the Test Does
1. **Setup**: Creates sample transaction data in DBFS
2. **Execute**: Runs Ardent model to create and execute a Databricks notebook
3. **Validate**: Checks that Delta table and data files were created correctly
4. **Cleanup**: Removes test data and files (but preserves existing clusters)

### Cluster Management
- If `DATABRICKS_CLUSTER_ID` is provided, uses that cluster (never deletes it)
- If not provided, creates a temporary single-node cluster for testing
- Only deletes clusters that were created by the test itself

## Running the Test

```bash
# Set environment variables first
export DATABRICKS_HOST=your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-token-here

# Run the test
pytest -sv -k "databricks"
```

## Troubleshooting

### Common Issues
1. **"Missing required environment variables"**: Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
2. **"Catalog does not exist"**: The test uses `hive_metastore` which should always exist
3. **"Cluster not found"**: Check your `DATABRICKS_CLUSTER_ID` is correct
4. **"Permission denied"**: Ensure your token has cluster and DBFS permissions

### Debug Mode
For verbose output during testing, run:
```bash
pytest -sv -k "databricks" --tb=long
```