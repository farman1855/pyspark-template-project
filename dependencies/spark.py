"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

from pyspark.sql import SparkSession


class Spark:
    def __init__(self):
        pass

    @staticmethod
    def start_spark(app_name='my_spark_app', master='local[*]'):
        spark = SparkSession.builder \
            .master(master) \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()
        return spark
