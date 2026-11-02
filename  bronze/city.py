from pyspark import pipelines as dp #imports spark declarative pipelines api
from pyspark.sql.functions import col, current_timestamp

SOURCE_PATH = "/Volumes/transportation/default/transport_data/city.csv"

#decorator which calls the city_bronze function
@dp.materialized_view(
    #dataframe returned by the function stored as the bronze city table
    name="transportation.bronze.city",
    comment="city raw data processing",
    table_properties={
        "quality":"bronze",
        "layer":"bronze",
        "source_format":"csv",
        "delta.enableChangeDataFeed":"true",
        "delta.autoOptimize.optimizeWrite":"true",
        "delta.autoOptimize.autoCompact":"true"
    }

)

#this function describes how the dataset is built
def city_bronze():
    df= spark.read.format("csv").option("header","true").option("mergeSchema","True").option("mode","PERMISSIVE").option("mergeSchema","True").option("columnNameOfCorruptRecord","_corrupt_record").load(SOURCE_PATH)
    
    df=df.withColumn("file_name",col("_metadata.file_path")).withColumn("ingest_datetime",current_timestamp())

    return df