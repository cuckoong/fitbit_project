"""
Author: Xiaoli Wu
Date: 2023-01
Description: Test processing.py
This is to test the processing.py in src folder
"""
# import packages
import logging
import pytest
from pyspark.sql import SparkSession
from src.quality_check import unique_check, non_null_check, data_type_check
from pyspark.sql.types import (
    LongType,
    IntegerType,
    FloatType,
    TimestampType,
    StringType,
    DoubleType,
)

# set up the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# spark session
spark = SparkSession.builder.getOrCreate()


@pytest.fixture
def fitbit_df():
    df = spark.read.parquet("data/processed/fitbit_table.parquet")
    return df


@pytest.fixture
def panas_df():
    df = spark.read.parquet("data/processed/panas_table.parquet")
    return df


@pytest.fixture
def personality_df():
    df = spark.read.parquet("data/processed/personality_table.parquet")
    return df


@pytest.fixture
def fact_df():
    df = spark.read.parquet("data/processed/facts.parquet")
    return df


def test_unique_check(panas_df, personality_df, fitbit_df, fact_df):
    # test the unique constraint
    assert unique_check(panas_df, "panas_id")
    assert unique_check(personality_df, "personality_id")
    assert unique_check(fitbit_df, "fitbit_id")
    assert unique_check(fact_df, "id")


def test_non_null_check(panas_df, personality_df, fitbit_df, fact_df):
    # test the non-null constraint
    assert non_null_check(panas_df)
    assert non_null_check(personality_df)
    assert non_null_check(fitbit_df)
    assert non_null_check(
        fact_df,
        ["id", "panas_id", "personality_id", "userId", "year", "month", "day"],
    )


def test_data_type_check(panas_df, personality_df, fitbit_df, fact_df):
    # test the data type constraint
    # Filter columns starting with 'P1'
    p1_columns = [
        field for field in panas_df.schema if field.name.startswith("P1")
    ]
    for field in p1_columns:
        data_type_check(panas_df, {field.name: IntegerType})

    assert data_type_check(
        panas_df,
        {
            "userId": StringType,
            "panas_id": LongType,
            "dateTime": TimestampType,
            "year": IntegerType,
            "month": IntegerType,
            "day": IntegerType,
        },
    )

    assert data_type_check(
        personality_df,
        {
            "userId": StringType,
            "personality_id": LongType,
            "extraversion": FloatType,
            "agreeableness": FloatType,
            "conscientiousness": FloatType,
            "stability": FloatType,
            "intellect": FloatType,
        },
    )

    assert data_type_check(
        fitbit_df,
        {
            "userId": StringType,
            "fitbit_id": LongType,
            "dateTime": TimestampType,
            "year": IntegerType,
            "month": IntegerType,
            "day": IntegerType,
            "type": StringType,
            "value": FloatType,
        },
    )

    assert data_type_check(
        fact_df,
        {
            "id": LongType,
            "panas_id": LongType,
            "personality_id": LongType,
            "userId": StringType,
            "year": IntegerType,
            "month": IntegerType,
            "day": IntegerType,
            "mean_steps": DoubleType,
            "mean_calories": DoubleType,
            "mean_distance": DoubleType,
            "sd_steps": DoubleType,
            "sd_calories": DoubleType,
            "sd_distance": DoubleType,
        },
    )
