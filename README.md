# Install packages
Run the below command from the project working directory
`python3 -m venv .venv`
`source .venv/bin/activate` (mac or linux)
`pip install -r requirements.txt`

# Run pipeline script
Run the below command from the project working directory
`python `src/process.py
### Note: Ensure the raw data is extracted and copied to the location `resources/`, e.g. `recources/data/logs` for the logs data

# Tests
Run the below command from the project working directory
`pytest`

# The script performs following tasks
Load and transform the logs, SKUs, and market price raw datasets.
Extract the details from logs dataset, replace null values for timestamp and drop duplicate datasets. 
Fills in missing dates for market price data.
Extracts and flattens nested JSON fields for SKUs.
Store logs, SKUs, and market price datasets in parquet format after cleaning and extracted the fields in the datasets.
Combines datasets into a single denormalized DataFrame.
Perform analytical queries to identify highest transaction volumes and most profitable brands.


# Assumptions
1. All the price values in the market_prices dataset are effective for each day. If the price is updated weekly, 
monthly, quarterly, or yearly, the value remains effective from the day it is updated until the next available price update.
2. Improper trace ids or details in logs dataset are ignored for calculation the data insights.
3. Total 3 inventory results are failing, it might be because of improper details in the logs dataset.
