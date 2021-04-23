import re

from pyspark.sql import functions as F
from pyspark.sql.functions import udf


class Transform(object):
    """
    ETL Transform component
    """

    def __init__(self):
        pass

    def execute(self, df,log):
        """Transform original dataset.

        :param df: Dataframe for transformation
        :return: Transformed DataFrame.
        """
        log.warn("Data Transformation Started.")
        try:
            convertUDF = udf(lambda z: self.to_minutes(z))
            dataframe = df.withColumn("prep_time", convertUDF(df.prepTime)).withColumn("cook_time", convertUDF(df.cookTime))
            dataframe = dataframe.fillna({'cook_time': 0, 'prep_time': 0})
            dataframe = dataframe.withColumn("total_cook_time", dataframe.cook_time + dataframe.prep_time)
            dataframe = dataframe.withColumn("difficulty", F.when(dataframe.total_cook_time < 30, "easy").when(
                (dataframe.total_cook_time > 30) & (dataframe.total_cook_time < 60), "medium").when(
                dataframe.total_cook_time > 60, "hard").otherwise("unknown"))
            dataframe = dataframe.select("difficulty", "total_cook_time").where(
                    F.lower(F.col("ingredients")).like("%beef%"))
            dataframe = dataframe.groupBy("difficulty").agg(
                    F.round(F.avg('total_cook_time'), 2).alias("avg_total_cooking_time")).sort(F.asc("difficulty"))
            log.warn("Data is Transformed")
            return dataframe
        except:
            log.warn("Dataframe format is not correct for transformation")

    @staticmethod
    def to_minutes(time):
        if re.findall(r'^(CT|PT)\d*H*\d*M$', time):
            temp = time.replace("CT", "").replace("H", "*60+").replace("PT", "").replace("M", "*1")
            return eval(temp)
        elif re.findall(r'^(CT|PT)\d*H$', time):
            temp = time.replace("PT", "").replace("CT", "").replace("H", "*60")
            return eval(temp)
