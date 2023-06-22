from typing import Optional

from pyspark.sql import SparkSession


def get_log4j_logger(spark: SparkSession):
    """
    Gets a logger needed for logging useful information
    :param spark:  A Spark Session
    :return: A log4j logger for Spark logging
    """
    log4j_logger = spark.sparkContext._jvm.org.apache.log4j
    return log4j_logger.LogManager.getLogger(__name__)


def get_spark_session(app_name: Optional[str] = None) -> SparkSession:
    """
    Gets / Generates a Spark Session
    :param app_name: The Spark application name. This parameter is optional.
    :return: A Spark session
    """
    builder = (
        SparkSession.builder.appName(f"{app_name}")
        if app_name
        else SparkSession.builder
    )
    return builder.getOrCreate()
