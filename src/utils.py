from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

spark = (
    SparkSession.builder
    .appName("M5-Demand-Forecasting")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)


spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

def read_data(path:str,format:str="parquet")->DataFrame:
    """takes in path and data format and returns spark DataFrame"""

    if format.lower() == "csv":
        return spark.read.format("csv").option("header",True).load(path)

    elif format.lower() == "table":
        return spark.read.table(path)

    elif format.lower() == "parquet":
        return spark.read.parquet(path)

    elif format.lower() == "delta":
        return spark.read.load(path)


def write_data(df:DataFrame,path:str,partition_cols:list[str]|str)->None:
    """taken in spark Dataframe, path where data has to be stored, 
    partition columns and saves the data to given path in parquet format"""

    df.write.format("parquet").partitionBy(*partition_cols).mode('overwrite').save(path)




