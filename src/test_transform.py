import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, year, quarter, sum as _sum, when

expected_values = {
    'WINE-OPU-001': 8,
    'WINE-OPU-003': 18592,
    'WINE-LAF-004': 5676,
    'WHKY-GLE-018': 3546,
    'BRBN-MAK-024': 130049
}

# Initialize Spark session for testing
@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("pytest-pyspark") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

# Fixture to create sample DataFrame
@pytest.fixture
def inventory_report(spark_session):
    return spark_session.read.parquet('results/denormalize_data/') \
        .select('code', 'transaction_type', 'units') \
        .withColumn('units',
                                     when(col('transaction_type') == 'sell',
                                          -col('units')).otherwise(col('units'))) \
                        .groupBy('code').agg(_sum('units').alias('total')) \
                        .rdd.collectAsMap()

# Test cases
def test_inventory_reports_wine_opu_001(inventory_report):

    assert inventory_report['WINE-OPU-001'] + expected_values['WINE-OPU-001'] >= 0

def test_inventory_reports_wine_opu_003(inventory_report):

    assert inventory_report['WINE-OPU-003'] + expected_values['WINE-OPU-003'] >= 0

def test_inventory_reports_wine_laf_004(inventory_report):

    assert inventory_report['WINE-LAF-004'] + expected_values['WINE-LAF-004'] >= 0

def test_inventory_reports_whky_gle_018(inventory_report):

    assert inventory_report['WHKY-GLE-018'] + expected_values['WHKY-GLE-018'] >= 0

def test_inventory_reports_brbn_mak_024(inventory_report):

    assert inventory_report['BRBN-MAK-024'] + expected_values['BRBN-MAK-024'] >= 0
