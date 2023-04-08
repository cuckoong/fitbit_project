"""
Author: Xiaoli Wu
Date: 2023-01
Description: Test processing.py
This is to test the processing.py in src folder
"""
# import packages
import os
import logging
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.processing import (
    drop_null_columns,
    extract_time,
    update_schema_nullable,
    clean_data,
)

# set up the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# test spark dataframe
spark = SparkSession.builder.getOrCreate()


@pytest.fixture
def one_missing_df():
    # with one missing value
    data = [(1, "A", 1), (2, "B", 2), (None, "C", 3), (4, "D", 4), (5, "E", 5)]
    columns = ["id", "name", "age"]
    return spark.createDataFrame(data=data, schema=columns)


@pytest.fixture
def one_dup_df():
    data = [(1, "A", 1), (1, "B", 2), (3, "C", 3), (4, "D", 4), (4, "D", 4)]
    columns = ["id", "name", "age"]
    return spark.createDataFrame(data=data, schema=columns)


@pytest.fixture
def one_col_null_df():
    # with one column with all null values
    data = [
        (1, "A", None),
        (2, "B", None),
        (3, "C", None),
        (4, "D", None),
        (5, "E", None),
    ]

    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("age", IntegerType(), nullable=True),
        ]
    )

    return spark.createDataFrame(data=data, schema=schema)


@pytest.fixture
def no_missing_or_dup_df():
    # with no missing value or duplicates
    data = [(1, "A", 1), (2, "B", 2), (3, "C", 3), (4, "D", 4), (5, "E", 5)]

    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("age", IntegerType(), nullable=True),
        ]
    )
    return spark.createDataFrame(data=data, schema=schema)


@pytest.fixture
def date_df():
    # with one column is 'dateTime'
    data = [
        (1, "2020-01-01", 1),
        (2, "2020-01-01", 2),
        (3, "2020-01-01", 3),
        (4, "2020-01-01", 4),
        (5, "2020-01-01", 5),
    ]

    columns = ["id", "dateTime", "age"]
    return spark.createDataFrame(data=data, schema=columns)


def test_clean_data(one_missing_df, one_dup_df, no_missing_or_dup_df):
    # test the clean_data function
    clean_df = clean_data(one_missing_df)
    assert clean_df.count() == (one_missing_df.count() - 1)

    clean_df = clean_data(one_dup_df)
    assert clean_df.count() == (one_dup_df.count() - 1)

    clean_df = clean_data(no_missing_or_dup_df)
    assert clean_df.count() == no_missing_or_dup_df.count()


def test_drop_null_columns(no_missing_or_dup_df, one_col_null_df):
    clean_df = drop_null_columns(no_missing_or_dup_df)
    assert len(clean_df.columns) == len(no_missing_or_dup_df.columns)

    clean_df = drop_null_columns(one_col_null_df)
    assert len(clean_df.columns) == len(one_col_null_df.columns) - 1


def test_extract_time(no_missing_or_dup_df, date_df):
    # case one
    with pytest.raises(ValueError, match="dateTime column does not exist"):
        extract_time(no_missing_or_dup_df)

    # case two
    df = extract_time(date_df).toPandas()
    # check year, month, day in the new columns
    assert "year" in df.columns
    assert "month" in df.columns
    assert "day" in df.columns


def test_update_schema_nullable(no_missing_or_dup_df):
    # case one: check original schema
    schema = no_missing_or_dup_df.schema
    for field in schema:
        assert field.nullable

    # case two: check updated schema
    spark = SparkSession.builder.getOrCreate()
    updated_schema = update_schema_nullable(
        no_missing_or_dup_df, spark_session=spark
    ).schema
    for field in updated_schema:
        assert not field.nullable


def test_save_data():
    # check if the file is saved
    assert os.path.exists("data/processed/fitbit_table.parquet")
    assert os.path.exists("data/processed/personality_table.parquet")
    assert os.path.exists("data/processed/panas_table.parquet")
    assert os.path.exists("data/processed/facts.parquet")
