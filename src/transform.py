from pyspark.sql.functions import from_unixtime, col, split, when, regexp_extract, \
    date_trunc, length, lit, trim, lag, sequence, explode, lead, expr, rank, month, year, quarter, weekofyear
from pyspark.sql.functions import sum as _sum, max as _max
from pyspark.sql.window import Window


def explode_date_values(df):
    # Define a window specification
    window_spec = Window.partitionBy('code').orderBy("date")

    # Add a column with the previous timestamp
    prev_ts_df = df.withColumn("temp_timestamp", expr("date - INTERVAL 1 DAY")) \
        .withColumn("prev_timestamp", lead("temp_timestamp").over(window_spec))

    # Generate a sequence of dates between prev_timestamp and timestamp
    seq_df = prev_ts_df.withColumn(
        "date_sequence",
        sequence(
            col("prev_timestamp"),  # Start date
            col("date")  # End date
        )
    ).drop('temp_timestamp')

    # Explode the date_sequence column
    exploded_df = seq_df.withColumn("date", explode(col("date_sequence"))).drop('prev_timestamp', 'date_sequence')

    # Add missing rows after the explode operation
    window = Window.partitionBy('code').orderBy(df.date.desc())
    rank_df = df.withColumn('rank', rank().over(window)).filter(col('rank') == 1).drop('rank')
    anti_join_df = rank_df.join(exploded_df, 'date', 'anti')

    cols = exploded_df.columns
    return exploded_df.union(anti_join_df.select(cols))


def transform_logs_data(df):
    # Extract transaction_type, product code and units from `details` column after filtering `message_type` as ORDER
    trans_df = df.select('trace_id', 'details').filter(col('message_type') == 'ORDER') \
        .withColumn("transaction_type", when(col("details").isNotNull(), trim(split(col("details"), " ").getItem(0)))) \
        .withColumn("code", when(col("details").isNotNull(), trim(split(col("details"), " ").getItem(1)))) \
        .withColumn("units",
                    when(col("details").isNotNull(), trim(split(col("details"), " ").getItem(2)).cast("int"))).drop(
        'details')

    # Join the extracted details with the logs df
    join_df = df.join(trans_df, 'trace_id', 'inner')

    # Fix the missing timestamp values in few rows with the max time for each trace ID
    window = Window.partitionBy('trace_id')
    fix_null_df = join_df.withColumn('timestamp', _max('timestamp').over(window))

    # Create date column with truncating timestamp to date, so that it can be joined with the market price dataset
    final_df =  fix_null_df.withColumn('date', date_trunc('day', col('timestamp')))

    return final_df.dropDuplicates()


def transform_skus_data(df):
    # Extract Json fields from skus data
    return df.withColumn('product_category_name', col('product_category').getItem('name')) \
        .withColumn('region_name', col('region').getItem('name')) \
        .withColumn('country_name', col('region').getItem('country').getItem('name')) \
        .withColumn('sub_varietal_name', col('sub_varietal').getItem('name')) \
        .withColumn('brand_name', col('brand').getItem('name')) \
        .withColumn('abv', col('attribute_json').getItem('abv')) \
        .withColumn('bottle_ml', col('attribute_json').getItem('bottle_ml')) \
        .withColumn('year', col('attribute_json').getItem('year')) \
        .withColumn('age', col('attribute_json').getItem('age')) \
        .withColumn('grape_variety', col('attribute_json').getItem('grape_variety')) \
        .drop('product_category', 'region', 'sub_varietal', 'brand', 'attribute_json')


def transfer_market_price_data(df):
    # Convert long nanoseconds ts to timestamp in seconds
    # extract product code
    # drop unused column
    # truncate timestamp to date column
    mp_df = df.withColumn("timestamp", from_unixtime(col("timestamp")/1_000_000_000)) \
        .withColumn('code', regexp_extract('quote_id', r'^([A-Z]+-[A-Z]+-\d{3})-(\d+)$', 1)) \
        .drop('__index_level_0__') \
        .withColumn('date', date_trunc('day', col('timestamp')))

    # Explode missing dates and prices for products which are updated monthly, weekly, quarterly and yearly
    return explode_date_values(mp_df)


def denormalize_data(logs, skus, mp):
    # Filter only successfully completed rows, filter null values and ignore improper trace ids for logs dataset
    cols = ['trace_id', 'code', 'transaction_type', 'units', 'product_category_name', 'region_name', 'country_name',
            'sub_varietal_name', 'brand_name', 'abv', 'bottle_ml', 'year', 'age', 'grape_variety', 'price_usd', 'date']
    logs_df = logs.filter(col('details') == 'COMPLETED') \
        .withColumn('length', length(col('trace_id'))) \
        .filter(col('trace_id').isNotNull()) \
        .filter(col('length') <= 16)

    # Join all the datasets
    join_df = logs_df.join(skus, 'code') \
        .join(mp, ['code', 'date'], 'left') \
        .filter(col('price_usd').isNotNull()) \
        .filter(col('date').isNotNull()) \
        .select(cols)

    return join_df


def highest_transaction_volume_per_region_each_sku(df, partition, group):
    cols = ['code', 'units', 'date', 'region_name']
    window_spec = Window.partitionBy(*partition).orderBy(col("volume").desc())
    return df.select(cols) \
        .withColumn('year', year(col('date'))) \
        .withColumn('quarter', quarter(col('date'))) \
        .groupBy(group) \
        .agg(_sum('units').alias('volume')) \
        .withColumn("rank", rank().over(window_spec)) \
        .filter(col("rank") == 1).drop("rank")


def most_profitable_two_brands_first_21_weeks_2024(df):
    cols = ['code', 'date', 'price_usd', 'transaction_type', 'units', 'brand_name']
    return df.select(cols) \
        .withColumn('year', year('date')) \
        .withColumn('week', weekofyear('date')) \
        .withColumn('month', month('date')) \
        .filter((col('year') == 2024) & (col('month') != 12) & (col('week') <= 21)) \
        .withColumn("profit", when(col("transaction_type") == "sell", col("units") * col("price_usd"))
                .otherwise(-col("units") * col("price_usd"))) \
        .groupBy("brand_name").agg(_sum("profit").alias("total_profit")) \
        .orderBy(col('total_profit').desc()).limit(2)
