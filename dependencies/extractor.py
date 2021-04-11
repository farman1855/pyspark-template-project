from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, StructField


def cleaned_udf(df):
    for column in df.columns:
        cleaned_df = df.withColumn(column, F.regexp_replace(column, '[\x7f\x80]', ""))
    return cleaned_df


def get_schema():
    return StructType([
        StructField('cookTime', StringType(), True),
        StructField('datePublished', StringType(), True),
        StructField('description', StringType(), True),
        StructField('image', StringType(), True),
        StructField('ingredients', StringType(), True),
        StructField('name', StringType(), True),
        StructField('date_published', StringType(), True),
        StructField('prepTime', StringType(), True),
        StructField('recipeYield', StringType(), True),
        StructField('url', StringType(), True)
    ])


class Extractor:
    """
    Data Extractor that will load data
    """

    def __init__(self, uri):
        self.uri = uri;

    def execute(self, log, spark):
        """Read list of Json Files in spark dataframe.
            :param log: Logging object to log data
            :param spark: Spark Session
            :return: Spark Dataframe
            """
        try:
            log.info("Data Loading Started")
            df = (
                spark
                    .read
                    .schema(get_schema())
                    .json(self.uri)
            )
            cleaned_df = cleaned_udf(df)
            log.info("Lets check data quality by counting null values from all columns")
            validation_df=cleaned_df.select([F.count(F.when(F.isnan(c), c)).alias(c) for c in cleaned_df.columns])
            validation_df.show()
            return cleaned_df

        except:
            print('No such file in current directory')
            return None
