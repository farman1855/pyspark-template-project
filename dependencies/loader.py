




class Load(object):
    """
    ETL Load Component
    """

    def __init__(self, load_path, partition_column):
        self.load_path = load_path;
        self.partition_column = partition_column;

    def execute(self, df, log):
        """Load data
        :param log: logging component
        :param df: DataFrame to load into parquet hive table
        :return: None
        """
        log.info("Data Writing is started.")
        try:
            df.write.format('csv').mode('append').partitionBy(self.partition_column).save(self.load_path)
        except:
            log.error("There is a problem in data writing.")
