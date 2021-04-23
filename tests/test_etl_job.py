"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""

import os
import unittest
from configparser import ConfigParser

from dependencies.extractor import Extractor
from dependencies.transformer import Transform
from dependencies.logging import Log4j
from dependencies.spark import Spark


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """
    def setUp(self):
        """Start Spark, define config and path to test data
        """
        config = ConfigParser()
        base_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
        config.read(base_path + '/configs/config.ini')
        self.uri = base_path + config.get('extractor', 'test_input')
        self.spark = Spark().start_spark(app_name='test')
        self.log = Log4j(self.spark)

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transformation(self):
        """Testing transformation
        Expectation: Need to provide transformation result as a dataset with 22 columns
        """

        df = Extractor(self.uri).execute(self.log, self.spark)
        df = Transform().execute(df, self.log)
        result = df.select('avg_total_cooking_time', 'difficulty').collect()

        result_list = [(row['avg_total_cooking_time'], row['difficulty']) for row in result]

        expected_list = [(21.0, 'easy'),
                         (70.0, 'hard'),(50.0, 'medium')]

        print(result_list)

        self.assertEqual(result_list, expected_list)

    def test_extract(self):
        """Testing Extract
        Expectation: Should Load all the records and give count of all rows
        """

        df = Extractor(self.uri).execute(self.log, self.spark)
        total_count = df.count()
        expected_count = 4

        self.assertEqual(total_count, expected_count)


if __name__ == '__main__':
    unittest.main()
