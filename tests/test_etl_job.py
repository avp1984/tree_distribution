"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for various analytics methods for `the planted trees analytics in San Francisco area`
defined in etl_job.py. It makes use of a local version of PySpark that is bundled with the PySpark package.
"""

import unittest

import json

from jobs.etl_job import TreeDistributionAnalyzer


class SparkETLTests(unittest.TestCase):
    """Test suite for tree analytics in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config, master input data frame and path to test data
        """
        self.config = json.loads("""{  "input-file-format": "csv",
          "input-dir": "/Users/arunvasu/Downloads/san_francisco_street_trees.csv",
          "output-dir": "/Users/arunvasu/Downloads/tree-distributions",
          "input-delimiter": ",",
          "input-header": "true",
          "infer-schema": "true"}""")
        self.test_data_path = 'tests/test-data/'
        self.test_data_validation_path = 'tests/test-data/validation-data/'
        self.executor = TreeDistributionAnalyzer()

        self.input_data = (
            self.executor.spark
                .read
                .option("inferSchema", "true")
                .option("header", "true")
                .option("sep", ",")
                .option("isDirectory", "true")
                .csv(self.test_data_path + 'input-data'))

    def tearDown(self):
        """Stop Spark
        """
        #self.executor.spark.stop()


    def test_find_banyan_trees(self):
        """Test find_banyan_trees()

        Using small chunks of input data and expected output data, we
        test the find_banyan_trees() method to make sure it's working as
        expected.
        """

        expected_data = (
            self.executor.spark
            .read
            .option("inferSchema", "true")
            .option("header", "true")
            .option("sep", ",")
            .option("isDirectory", "true")
            .csv(self.test_data_validation_path + 'find_banyan_trees'))

        data_transformed = self.executor.create_output_dataframe(self.executor.find_banyan_trees(self.input_data))
        #data_transformed = self.executor.find_banyan_trees(self.input_data)
        #data_transformed.show(truncate=False)
        expected_data.show(truncate=False)
        print(data_transformed)

        self.assertEqual(expected_data.columns, data_transformed.columns)
        self.assertEqual(expected_data.count(), data_transformed.count())
        exp_count = expected_data.select('BanyanTreeCount').collect()
        actual_count = data_transformed.select('BanyanTreeCount').collect()
        self.assertEqual((exp_count),(actual_count))
    #
    # def test_find_plum_trees(self):
    #     """Test find_plum_trees()
    #
    #     Using small chunks of input data and expected output data, we
    #     test the find_plum_trees() method to make sure it's working as
    #     expected.
    #     """
    #
    #     expected_data = (
    #         self.executor.spark
    #         .read
    #         .option("inferSchema", "true")
    #         .option("header", "true")
    #         .option("sep", ",")
    #         .option("isDirectory", "true")
    #         .csv(self.test_data_validation_path + 'find_plum_trees'))
    #
    #     data_transformed = self.executor.find_plum_trees(self.input_data)
    #     data_transformed.show(truncate=False)
    #
    #     self.assertEqual(expected_data.columns, data_transformed.columns)
    #     self.assertEqual(expected_data.count(), data_transformed.count())
    #     exp_count = expected_data.select('CherryPlumTrees').collect()
    #     actual_count = data_transformed.select('CherryPlumTrees').collect()
    #     self.assertEqual((exp_count),(actual_count))
    #
    # def test_find_most_common_trees(self):
    #     """Test find_most_common_trees()
    #
    #     Using small chunks of input data and expected output data, we
    #     test the find_most_common_trees() method to make sure it's working as
    #     expected.
    #     """
    #
    #     expected_data = (
    #         self.executor.spark
    #         .read
    #         .option("inferSchema", "true")
    #         .option("header", "true")
    #         .option("sep", ",")
    #         .option("isDirectory", "true")
    #         .csv(self.test_data_validation_path + 'find_most_common_trees'))
    #
    #     data_transformed = self.executor.find_most_common_trees(self.input_data)
    #     data_transformed.show(truncate=False)
    #
    #     self.assertEqual(expected_data.columns, data_transformed.columns)
    #     self.assertEqual(expected_data.count(), data_transformed.count())
    #     exp_trees = expected_data.select('tree_type').collect()
    #     actual_trees = data_transformed.select('tree_type').collect()
    #     self.assertEqual((exp_trees),(actual_trees))
    #
    # def test_find_most_trees_address(self):
    #     """Test find_most_trees_address()
    #
    #     Using small chunks of input data and expected output data, we
    #     test the find_most_trees_address() method to make sure it's working as
    #     expected.
    #     """
    #
    #     expected_data = (
    #         self.executor.spark
    #         .read
    #         .option("inferSchema", "true")
    #         .option("header", "true")
    #         .option("sep", ",")
    #         .option("isDirectory", "true")
    #         .csv(self.test_data_validation_path + 'find_most_trees_address'))
    #
    #     data_transformed = self.executor.find_most_trees_address(self.input_data)
    #     data_transformed.show(truncate=False)
    #
    #     self.assertEqual(expected_data.columns, data_transformed.columns)
    #     self.assertEqual(expected_data.count(), data_transformed.count())
    #     exp_most_trees_address = expected_data.select('address').collect()
    #     actual_most_trees_address = data_transformed.select('address').collect()
    #     self.assertEqual((exp_most_trees_address),(actual_most_trees_address))

if __name__ == '__main__':
    unittest.main()
