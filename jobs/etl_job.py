"""
etl_job.py
~~~~~~~~~~
Spark job can be submitted to a Spark cluster (or locally) using
the 'spark-submit' command found in the '/bin' directory of all Spark distributions

This example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

where,
 - packages.zip contains Python modules required by ETL job
 - etl_config.json is a text file sent to the cluster, containing a JSON object with all of the configuration parameters
 - etl_job.py contains the Spark application to be executed by a driver process on the Spark master node.

"""

from pyspark.sql.functions import col, desc, rank, split
from pyspark.sql.window import Window

from dependencies.spark import start_spark

class TreeDistributionAnalyzer:

    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='sf_tree_distribution_job',
        files=['configs/etl_config.json'])

    def main(self):
        """Main ETL script definition.
        This method initiate the ETL to findout the statistics of the tree distribution across San Francisco area
        and persist the results to disk
        :return: None
        """

        # log that main ETL job is starting
        self.log.warn('sf_tree_distribution_job is up-and-running')

        self.log.warn('Job parameters: {}'.format(self.config))

        # execute ETL pipeline to read the input data
        data = self.extract_input_data(self.spark, self.config)

        # find most commonly occurred trees
        most_common_trees = self.find_most_common_trees(data)
        self.save_output_data(most_common_trees, self.config['output-dir'] + "/most_common_trees")

        # find the address with maximum number of trees planted
        most_trees_in_location = self.find_most_trees_address(data)
        self.save_output_data(most_trees_in_location, self.config['output-dir'] + "/most_trees_in_location")

        # find number of Banyan trees which has a permit number
        count_banyan_trees = self.create_output_dataframe(self.find_banyan_trees(data))
        self.save_output_data(count_banyan_trees, self.config['output-dir'] + "/count_banyan_trees")

        # find the Plum trees which are DPW maintained
        count_plum_trees = self.find_plum_trees(data)
        self.save_output_data(count_plum_trees, self.config['output-dir'] + "/count_plum_trees")

        # log the success and terminate Spark application
        self.log.warn('test_etl_job is finished')
        self.spark.stop()
        return None


    def extract_input_data(self, spark, config):
        """Load data from CSV file format.

        :param spark: Spark session object.
        :param config: Job configurations
        :return: Spark DataFrame of the tree details
        """

        df = (spark.read.format(config['input-file-format']) \
            .option("inferSchema",config['infer-schema']) \
            .option("header", config['input-header']) \
            .option("sep", config['input-delimiter']) \
            .load(config['input-dir']))

        self.log.warn('Input data read and DataFrame loaded')
        return df


    def find_most_trees_address(self, df):
        """Find the adrress with most number of trees planted

        :param df: Input DataFrame containing all details of trees
        :return: DataFrame of the address with most number of trees
        """

        # dataframe of address and corresponding tree_id(s) and find the total count of trees in each address
        max_trees = df.select('address', 'tree_id').filter(df.address.isNotNull()).groupBy(col('address')).count().sort(
            desc("count"))

        # rank the addresses based on decreasing order of number of trees planted and find top address, which is rank 1
        max_trees_place = max_trees.withColumn("rank", rank().over(Window.orderBy(col("count").desc()))).filter(
            col("rank") == 1).select('address')

        self.log.warn('Found the address with most number of trees planted')

        return max_trees_place

    def find_most_common_trees(self, df):
        """Find the top 5 most commonly occurred tree  types in San Francisco area

        :param df: Input DataFrame containing all details of trees
        :return: Dataframe of top 5 common tree types
        """

        # create split rule to find the tree sub type
        split_col = split(df['species'], '::')

        # a dataframe of trees with only required fields
        comm_trees = df.select('species', 'tree_id').withColumn('tree_type', split_col.getItem(1))

        # filter out the trees with no or unknown sub types and get the count of valid sub types
        comm_tree_df = (comm_trees.select('tree_type').filter(col('tree_type') != '').groupBy('tree_type').count()
            .orderBy(col('count').desc()))

        # find the top 5 commonly occured tree types by using ranking
        most_comm_trees = comm_tree_df.withColumn("rank", rank().over(Window.orderBy(col("count").desc()))).filter(
            col("rank") <= 5).select('tree_type', 'count')

        self.log.warn('Found the top 5 most common trees in San Francisco')

        return most_comm_trees

    def find_plum_trees(self, df):
        """Find the number of `Cherry Plum` trees which are `DPW Maintained`

        :param df: Input DataFrame containing all details of trees
        :return: DataFrame of count of plum trees
        """

        """The `species` is in the format of <Type::Subtype>, then find the sub type by splitting it and filter on the 
        subtype to match the cherry plum subtype. Also check if the tree is DPW maintained.
        """
        plum_trees = (df.select('species', 'tree_id').filter(
        col('species').contains('Cherry Plum') & col('legal_status').like('DPW Maintained')))

        # find the total of trees in each cherry plum tree types
        plum_trees_df = plum_trees.select('species').groupBy('species').count()

        # find the total number of cherry plum trees by summing up from the individual sub type categories
        total_plum_trees = ((plum_trees_df.groupBy().sum().alias('plum')).
            withColumnRenamed('sum(count)', 'CherryPlumTrees'))

        self.log.warn('Found number of plum trees which are DPW Maintained')

        return total_plum_trees

    def find_banyan_trees(self, df):
        """Find the number of Banyan trees which are assigned a permit number

        :param df: Input DataFrame containing all details of trees
        :return: DataFrame of banyan trees
        """

        # The DF is filtered to match all the species of Banyan tree and if the tree has proper permit number
        banyan_tree = (df.select('species', 'permit_notes').filter(col('species').like('%Banyan Fig%') & col('permit_notes').rlike('\w+\s?\w+\s?\d+')))

        self.log.warn('Found number of Banyan trees which has a permit number')

        return banyan_tree

    def save_output_data(self, df, outfile_name):
        """Collect data and write to CSV to the specified output directory as a single CSV file.

        :param df: Input DataFrame containing all details of trees
        :param outfile_name: The output directory location
        :return: None
        """

        # save the result as a sing csv file with header
        (df
         .coalesce(1)
         .write
         .csv(outfile_name, mode="overwrite", header=True))
        return None

    def create_output_dataframe(self, df):
        """This is a utility for code modularity

        :param df: Input DataFrame containing all details of trees
        :param outfile_name: The output directory location
        :return: None
        """
        banyan_tree_count = self.spark.createDataFrame([[df.count()]]).withColumnRenamed('_1', 'BanyanTreeCount')
        return banyan_tree_count

# entry point for PySpark application
if __name__ == '__main__':
    treeAnalyzer = TreeDistributionAnalyzer()
    treeAnalyzer.main()
