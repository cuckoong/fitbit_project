import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import FloatType
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import monotonically_increasing_id

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.debug("test")


def create_spark_session():
    """
    Create a spark session
    :return: spark session
    """
    spark = (
        SparkSession.builder.config(
            "spark.jars.repositories", "https://repos.spark-packages.org/"
        )
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        )
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory", "1g")
        .enableHiveSupport()
        .getOrCreate()
    )

    return spark


def drop_null_columns(df):
    """
    Drop columns with all null values
    :param df: spark dataframe
    :return: spark dataframe with columns with all null values dropped
    """
    # drop all null columns
    expr = [
        F.count(F.when(F.col(column).isNotNull(), 1)).alias(column)
        for column in df.columns
    ]
    non_null_counts = df.agg(*expr).collect()[0].asDict()

    columns_to_drop = [
        column for column, count in non_null_counts.items() if count == 0
    ]
    df_cleaned = df.drop(*columns_to_drop)

    logger.info(f"drop columns from {columns_to_drop}")

    return df_cleaned


def extract_time(df):
    """
    Extract year, month, day from dateTime column
    :param df: spark dataframe with dateTime column
    :return: spark dataframe with year, month, day columns
    """
    # check if dateTime column exists
    if "dateTime" not in df.columns:
        logger.error("dateTime column does not exist")
        raise ValueError("dateTime column does not exist")

    # extract year, month, day columns from dateTime column
    df = (
        df.withColumn("year", year(df.dateTime))
        .withColumn("month", month(df.dateTime))
        .withColumn("day", dayofmonth(df.dateTime))
    )

    return df


def update_schema_nullable(df, column_names=None, spark_session=None):
    """
    Update the nullable property of a specific column in
     the schema of a DataFrame
    :param df: spark dataframe
    :param column_names: list of column names to update
    :param spark_session: spark session
    :return: spark dataframe with updated schema
    """
    if column_names is None:
        # convert all columns
        logger.info("Convert all columns to non-nullable")
        column_names = df.columns

    # Get the original schema
    original_schema = df.schema

    # Create a new schema with the nullable property of
    # a specific column set to False
    new_schema = StructType(
        [
            StructField(field.name, field.dataType, nullable=False)
            if field.name in column_names
            else field
            for field in original_schema
        ]
    )

    # Apply the new schema to the DataFrame
    df_with_new_schema = spark_session.createDataFrame(df.rdd, new_schema)

    return df_with_new_schema


def save_data(df, num, saved_path=None):
    """
    Save data to parquet file
    :param df: spark dataframe
    :param num: number of partitions
    :param saved_path: path to save data
    """
    if saved_path is None:
        logger.error("saved_path is None")
        raise ValueError("saved_path is None")

    logger.info(f"Save data to {saved_path}")
    # save partition by userId
    df.repartition(num, "userId").write.mode("overwrite").parquet(saved_path)


def clean_data(df):
    """
    Clean data by removing duplicates and missing values
    :param df: spark dataframe
    :return: spark dataframe with duplicates and missing values removed
    """
    # remove missing values
    logger.info("Removing rows with missing values")
    dropNA_df = df.dropna()

    # remove duplicates
    logger.info("Removing duplicate rows")
    clean_df = dropNA_df.dropDuplicates()

    return clean_df


def fitbit_data_processing(
    fitbit_df, fraction=1, fitbit_types=None, spark_session=None
):
    """
    Process fitbit data
    :param fitbit_df: spark dataframe
    :param fraction: fraction of data to process,
                    if 1 or None, process all data
    :param fitbit_types: list of fitbit types to process
    :param spark_session: spark session
    :return: spark dataframe with processed fitbit data
    """
    logger.info(f"Get data of {[t for t in fitbit_types]} from fitbit")

    # sampling for debug
    if fraction < 1:
        fitbit_df = fitbit_df.sample(fraction=fraction, seed=42)

    # check fitbit types are valid
    if fitbit_types is None:
        logger.error("fitbit_types is None")
        raise ValueError("fitbit_types is None")

    # check fitbit types are in fitbit_df
    data_types = fitbit_df.select("type").distinct().collect()
    data_types_list = [row.type for row in data_types]

    for t in fitbit_types:
        if t not in data_types_list:
            logger.error(f"{t} is not in fitbit_df")
            raise ValueError(f"{t} is not in fitbit_df")

    # filtering by types
    fitbit_subset = fitbit_df.filter(fitbit_df.type.isin(fitbit_types))

    # select related values
    fitbit_subset_flat = fitbit_subset.select(
        fitbit_subset["id.oid"].alias("userId"),
        fitbit_subset["data.dateTime"].alias("dateTime"),
        fitbit_subset["data.value"].alias("value"),
        fitbit_subset["type"].alias("type"),
    )

    # convert datatime type
    fitbit_subset_flat = fitbit_subset_flat.withColumn(
        "dateTime",
        to_timestamp(
            fitbit_subset_flat["dateTime"], format="yyyy-MM-dd'T'HH:mm:ss"
        ),
    )

    fitbit_subset_flat = fitbit_subset_flat.withColumn(
        "value", fitbit_subset_flat["value"].cast(FloatType())
    )

    # drop duplicates and null rows
    fitbit_subset_flat = clean_data(fitbit_subset_flat)

    # extract year, month, day columns from dateTime column
    logger.info("Converting dateTime to year, month, day")
    fitbit_subset_flat = extract_time(fitbit_subset_flat)

    # update nullable
    logger.info("updating schema")
    fitbit_subset_flat = update_schema_nullable(
        fitbit_subset_flat, spark_session=spark_session
    )

    logger.info("Done")
    return fitbit_subset_flat


def personality_processing(personality, spark_session=None):
    """
    Process personality data
    :param personality: spark dataframe
    :param spark_session: spark session
    :return: spark dataframe with processed personality data
    """
    logger.info("Get personality data from csv file")
    df = personality.withColumnRenamed("user_id", "userId")

    # select columns
    df.select(
        "userId",
        "extraversion",
        "agreeableness",
        "conscientiousness",
        "stability",
        "intellect",
    )

    df = update_schema_nullable(df, spark_session=spark_session)
    return df


def survey_panas_processing(survey_df, spark_session=None):
    """
    Process survey data
    :param survey_df: spark dataframe
    :param spark_session: spark session
    :return: spark dataframe with processed survey data
    """
    logger.info("Get panas data from survey")

    surveys_subset = survey_df.filter(survey_df.type == "panas")

    # flatten the id column as user_id
    surveys_flat = surveys_subset.select(
        "*", col("user_id.oid").alias("userId")
    ).drop("user_id", "_id")

    # flatten the data column
    surveys_flat = surveys_flat.select(
        "*",
        *[
            col("data").getField(field).alias(field)
            for field in surveys_flat.select("data.*").columns
        ],
    ).drop("data")

    # drop all null columns
    surveys_flat_cleaned = drop_null_columns(surveys_flat)

    # drop duplicates and missing data
    surveys_flat_cleaned = clean_data(surveys_flat_cleaned)

    # convert datatime type
    surveys_flat_cleaned = surveys_flat_cleaned.withColumn(
        "dateTime",
        to_timestamp(surveys_flat_cleaned["datestamp"], format="d/M/y H:m"),
    )

    # drop some columns
    surveys_flat_cleaned = surveys_flat_cleaned.drop(
        "groupTime770",
        "id",
        "interviewtime",
        "startdate",
        "submitdate",
        "datestamp",
    )

    # extract year, month, day columns from dateTime column
    surveys_flat_cleaned = extract_time(surveys_flat_cleaned)

    # update nullable
    surveys_flat_cleaned = update_schema_nullable(
        surveys_flat_cleaned, spark_session=spark_session
    )

    return surveys_flat_cleaned


def pipline():
    """
    Perform data extraction, transformation, and loading with pyspark;
    and then save the data to parquet files
    :return: None
    """
    logger.info("Starting pipeline")

    # create spark session
    spark = create_spark_session()

    # read data
    # read the data from mongodb and save it to spark dataframe
    surveys = (
        spark.read.format("com.mongodb.spark.sql.DefaultSource")
        .option("uri", "mongodb://localhost/rais_anonymized.surveys")
        .load()
    )

    fitbit = (
        spark.read.format("com.mongodb.spark.sql.DefaultSource")
        .option("uri", "mongodb://localhost/rais_anonymized.fitbit")
        .load()
    )

    # personality
    personality = spark.read.csv(
        "../data/scored_surveys/personality.csv", header=True, inferSchema=True
    ).drop("_c0")

    # extract fitbit data
    fitbit_df = fitbit_data_processing(
        fitbit_df=fitbit,  # fraction=0.0001,
        fitbit_types=["steps", "distance", "calories"],
        spark_session=spark,
    )

    # extract personality data
    personality_df = personality_processing(personality, spark_session=spark)

    # extract survey data
    panas_data = survey_panas_processing(surveys, spark_session=spark)

    # data transformation
    # transform panas data
    panas_table = panas_data.withColumn(
        "panas_id", monotonically_increasing_id()
    )
    save_data(
        panas_table, num=4, saved_path="../data/processed/panas_table.parquet"
    )

    # transform personality data
    personality_table = personality_df.withColumn(
        "personality_id", monotonically_increasing_id()
    )

    # cast the data type of personality columns to float
    personality_table = personality_table.select(
        "personality_id",
        "userId",
        personality_table["extraversion"].cast(FloatType()),
        personality_table["agreeableness"].cast(FloatType()),
        personality_table["conscientiousness"].cast(FloatType()),
        personality_table["stability"].cast(FloatType()),
        personality_table["intellect"].cast(FloatType())
    )

    save_data(
        personality_table,
        num=4,
        saved_path="../data/processed/personality_table.parquet",
    )

    # transform fitbit data
    fitbit_table = fitbit_df.withColumn(
        "fitbit_id", monotonically_increasing_id()
    )
    save_data(
        fitbit_table,
        num=4,
        saved_path="../data/processed/fitbit_table.parquet",
    )

    # fitbit_fact table (fact table)
    panas_table.createOrReplaceTempView("panas_table")
    personality_table.createOrReplaceTempView("personality_table")
    fitbit_table.createOrReplaceTempView("fitbit_table")

    query = """
        SELECT monotonically_increasing_id() as id,
        pa.panas_id,
        pe.personality_id,
        pa.userId,
        pa.year,
        pa.month,
        pa.day,
        AVG(CASE WHEN fi.type = 'distance' THEN fi.value ELSE 0 END) as mean_distance,
        AVG(CASE WHEN fi.type = 'calories' THEN fi.value ELSE 0 END) as mean_calories,
        AVG(CASE WHEN fi.type = 'steps' THEN fi.value ELSE 0 END) as mean_steps,
        STDDEV(CASE WHEN fi.type = 'distance' THEN fi.value ELSE 0 END) as sd_distance,
        STDDEV(CASE WHEN fi.type = 'calories' THEN fi.value ELSE 0 END) as sd_calories,
        STDDEV(CASE WHEN fi.type = 'steps' THEN fi.value ELSE 0 END) as sd_steps
        FROM fitbit_table fi
        JOIN personality_table pe ON fi.userId = pe.userId
        JOIN panas_table pa
        ON fi.userId = pa.userId AND pa.dateTime - INTERVAL 7 DAYS <= fi.dateTime AND pa.dateTime > fi.dateTime
        GROUP BY pa.panas_id, pa.userId, pa.year, pa.month, pa.day, pe.personality_id
        ORDER BY pa.userId, pa.year, pa.month, pa.day
    """  # noqa

    fitbit_fact_table = spark.sql(query)
    save_data(
        fitbit_fact_table, num=4, saved_path="../data/processed/facts.parquet"
    )


if __name__ == "__main__":
    pipline()
