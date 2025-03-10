from log_session import logger

from pyspark.sql import SparkSession

# Initialize spark session
spark = (SparkSession.builder.appName('Ferovinum')
         .master("local[*]")
         .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
         .config("spark.sql.legacy.parquet.nanosAsLong", "true").getOrCreate())

logger.info('Initialized spark session...')
