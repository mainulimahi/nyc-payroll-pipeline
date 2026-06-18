import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, initcap, trim
from pyspark.sql.types import IntegerType, DoubleType, TimestampType, DateType


def get_spark_session(app_name: str = "NYCPayrollPipeline") -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


def process_data(spark: SparkSession, df: pd.DataFrame) -> pd.DataFrame:
    spark_df = spark.createDataFrame(df)
    spark_df = spark_df.dropDuplicates()

    # Skip numeric AND date/timestamp columns from text cleaning
    skip_types = (IntegerType, DoubleType, TimestampType, DateType)
    string_cols = [
        field.name for field in spark_df.schema.fields
        if not isinstance(field.dataType, skip_types)
    ]

    for column in string_cols:
        spark_df = spark_df.withColumn(column, trim(initcap(lower(col(column)))))

    result = spark_df.toPandas()
    spark.stop()
    return result