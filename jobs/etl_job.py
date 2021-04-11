"""
etl_job.py
~~~~~~~~~~
"""

from configparser import ConfigParser
from dependencies.extractor import Extractor
from dependencies.transformer import Transform
from dependencies.loader import Load
from dependencies.logging import Log4j
from dependencies.spark import Spark
import os


class Executor(object):
    def __init__(self):
        pass

    def run(self):
        spark = Spark.start_spark(app_name='my_etl_job')
        log = Log4j(spark)
        config = ConfigParser()
        base_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
        config.read(base_path + '/configs/config.ini')
        uri = base_path + config.get('extractor', 'input_files')
        df = Extractor(uri).execute(log, spark)
        df = Transform().execute(df, log)
        loader_path = base_path + config.get('loader', 'output_file')
        partition_cols = config.get('loader', 'partition_cols')
        Load(loader_path, partition_cols).execute(df, log)


def main():
    """
        Main ETL script definition.
        :return: None
        """

    print('etl_job is up-and-running')
    try:
        Executor().run()
    except KeyError as e:
        print("Some components are missing" + repr(e))

    print("ETL job is finished")


if __name__ == '__main__':
    main()
