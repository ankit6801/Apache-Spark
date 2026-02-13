#Unit test booking analysis

import pytest
import unittest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession
from booking_analysis import top_3_revenue,read_booking_summary


@pytest.fixture(scope="session")
def spark():
    #GLobal Spark Session passed as fixture
    return SparkSession.builder.getOrCreate()

def test_read_booking_summary(spark):
    #Get Actual Results
    summary_df = read_booking_summary(spark)
    record_loaded =  summary_df.count()
    #Assert the actuals with expected
    assert record_loaded  == 58

def test_top_3_revenue(spark):
    #Get Actual Results
    summary_df = read_booking_summary(spark)
    result_df = top_3_revenue(summary_df)
    #GEt the Expected results
    file_schema = "booked_by string, booking_date string, revenue double"
    expected_df = (
        spark.read.format("csv")
        .option("header",'true')
        .schema(file_schema)
        .load("/Volumes/dev_catalog/spark_db/datasets/spark_programming/data/top-3-days-test-data.csv")
        # .load("/Volumes/dev_catalog/spark_db/datasets/spark_programming/data/bookings.csv")
    )
    #Assert
    assertDataFrameEqual(expected_df,result_df)
