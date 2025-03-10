from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType, LongType


logs_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("message_type", StringType(), True),
    StructField("trace_id", StringType(), True),
    StructField("details", StringType(), True)
])


skus_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("product_category", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("region", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("country", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ]), True)
    ]), True),
    StructField("sub_varietal", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("brand", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("attribute_json", StructType([
        StructField("abv", DoubleType(), True),
        StructField("bottle_ml", DoubleType(), True),
        StructField("year", IntegerType(), True),
        StructField("age", IntegerType(), True),
        StructField("grape_variety", StringType(), True)
    ]), True)
])

mp_schema = StructType([
    StructField('timestamp', LongType(), nullable=True),
    StructField('quote_id', StructType(), nullable=True),
    StructField('price_usd', DoubleType(), nullable=True)
])