import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# set up the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# spark session
spark = SparkSession.builder.getOrCreate()


def unique_check(df, column_name):
    """
    Test the unique constraint of the data
    :param df: spark dataframe
    :param column_name: column name for the unique constraint check
    :return: True if the unique constraint is satisfied, otherwise False
    """
    try:
        df.createOrReplaceTempView("df")
        records = spark.sql(
            f"SELECT {column_name} FROM df GROUP BY {column_name} "
            f"HAVING COUNT({column_name}) > 1"
        )

        if records.count() > 0:
            logger.info(
                "unique constraint of column {} is violated".format(
                    column_name
                )
            )
            return False
        else:
            logger.info("unique constraint is satisfied")
            return True

    except Exception as e:
        logger.warning(e)


def non_null_check(df, column_name=None):
    """
    Test the non-null constraint of the data
    :param df: spark dataframe
    :param column_name: column name for the non-null constraint check
    :return: True if the non-null constraint is satisfied, otherwise False
    """
    if column_name is None:
        column_name = df.columns

    # Replace `df` with your DataFrame name
    null_check = df.select(
        [count(when(col(c).isNull(), c)).alias(c) for c in column_name]
    )

    # Get the columns containing null values
    columns_with_nulls = [
        c for c in null_check.columns if null_check.first()[c] > 0
    ]

    if columns_with_nulls:
        logger.warning(
            "Columns containing null values:" + ", ".join(columns_with_nulls)
        )
        return False
    else:
        logger.info("Non-null constraint is satisfied")
        return True


def data_type_check(df, data_type_dict):
    """
    Test the data type constraint of the data
    :param df: spark dataframe
    :param data_type_dict: a dictionary of data type for each column
    {column_name: data_type}
    :return: True if the data type constraint is satisfied, otherwise False
    """
    try:
        df.createOrReplaceTempView("df")
        for data_name, data_type in data_type_dict.items():
            current_datatype = df.schema[data_name].dataType
            if not isinstance(current_datatype, data_type):
                logger.warning(
                    "Data type of {} should be {}, but get {}".format(
                        data_name, data_type, current_datatype
                    )
                )
                return False

        logger.info("Data type constraint is satisfied")
        return True

    except Exception as e:
        logger.error(e)
