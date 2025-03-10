import os

from log_session import cdr
from log_session import logger
from spark_session import spark

from schemas import logs_schema, skus_schema, mp_schema

data_location = 'resources/data/'

# Load Log file
logs_df = spark.read.csv(os.path.join(cdr, data_location, 'logs/'), schema=logs_schema, sep=" | ", header=False)
logger.info('Loaded logs data...')
logger.info(f'Schema Logs data: {logs_df.printSchema}')

# Load SKUs data
skus_df = spark.read.json(os.path.join(cdr, data_location, 'skus/'), schema=skus_schema)
logger.info('Loaded skus data...')
logger.info(f'Schema skus data: {skus_df.printSchema}')

# Load market prices
mp_df = spark.read.parquet(os.path.join(cdr, data_location, 'market_prices/'), schema=mp_schema)
logger.info('Loaded market price data...')
logger.info(f'Schema market price data: {mp_df.printSchema}')
