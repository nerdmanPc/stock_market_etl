### Dependencies

- Python 3
- Sqlite 3

### API Key

You can get your free Alpha Vantage API key by following this [link](https://www.alphavantage.co/support/#api-key)

After that, you need to set the ALPHA_VANTAGE_KEY environment variable like so:

```bash
export ALPHA_VANTAGE_KEY=YOUR_KEY_HERE
```

### Warehouse Path

Just like with the API key, this ETL application uses an environment variable to control where the database will live. You can set it like so:

```bash
export ETL_WAREHOUSE_PATH='/path/to/warehouse.db'
```

### Setting the Warehouse up

First, edit the *migration/setup_companies_list.sql* file to include the tickers of the companies you want to track, then run:

```bash
python3 migration/migrate.py
```

### Updating the Warehouse

```bash
python3 src/av_update.py
```

This script will pull entries from the Alpha Vantage endpoint and add the new entries for each time series. You can run this periodically or before each analisys