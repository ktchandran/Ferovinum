import os
from logging import exception

from log_session import cdr
from load_raw import logs_df, skus_df, mp_df
from transform import transform_logs_data, transform_skus_data, transfer_market_price_data, denormalize_data, \
    highest_transaction_volume_per_region_each_sku, most_profitable_two_brands_first_21_weeks_2024
from log_session import logger
from spark_session import spark

from pyspark.sql.functions import col, year, quarter, sum as _sum, when


if __name__ == '__main__':
    logger.info('Started processing data...')

    try:
        # Process raw data and store the data in parquet format.

        logs = transform_logs_data(logs_df)
        logs.write.mode("overwrite").parquet(os.path.join(cdr, "results/logs/"))
        skus = transform_skus_data(skus_df)
        skus.write.mode("overwrite").parquet(os.path.join(cdr, "results/skus/"))
        market_price = transfer_market_price_data(mp_df)
        market_price.write.mode("overwrite").parquet(os.path.join(cdr, "results/market_price/"))

        # Denormalize the data and store the data in parquet format.

        full_df = denormalize_data(logs, skus, market_price)
        full_df.write.mode("overwrite").parquet(os.path.join(cdr, "results/denormalize_data/"))
        logger.info(f'Schema of denormalized data: {full_df.printSchema}')

        # Create insights from the final dataset.

        df = spark.read.parquet(os.path.join(cdr, "results/denormalize_data/"))

        location = os.path.join(cdr, "results/highest_transaction_volume_per_region_each_sku_per_quarter/")
        highest_transaction_volume_per_region_each_sku(
            df,
            ['code', 'quarter', 'year'],
            ['code', 'region_name', 'year', 'quarter']) \
            .repartition(1) \
            .write.mode("overwrite").option("header", "true") \
            .csv(location)
        logger.info(
            f"""Results generated for highest_transaction_volume_per_region_each_sku_per_quarter in location {location}""")

        location = os.path.join(cdr, "results/highest_transaction_volume_per_region_each_sku_per_year/")
        highest_transaction_volume_per_region_each_sku(
            df,
            ['code', 'year'],
            ['code', 'region_name', 'year']) \
            .repartition(1) \
            .write.mode("overwrite").option("header", "true") \
            .csv(location)
        logger.info(
            f"""Results generated for highest_transaction_volume_per_region_each_sku_per_year in location {location}""")

        location = os.path.join(cdr, "results/most_profitable_two_brands_first_21_weeks_2024/")
        most_profitable_two_brands_first_21_weeks_2024(df)\
            .repartition(1) \
            .write.mode("overwrite").option("header", "true") \
            .csv(location)
        logger.info(
            f"""Results generated for most_profitable_two_brands_first_21_weeks_2024 in location {location}""")
        logger.info('Successfully completed!')

    except exception as e:
        logger.error('The pipeline has been failed with the error ', exc_info=True)
