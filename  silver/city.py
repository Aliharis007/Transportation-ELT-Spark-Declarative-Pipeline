from pyspark import pipelines as dp 
from pyspark.sql import functions as F

@dp.materialized_view(
    name="transportation.silver.city",
    comment="Cleaned and standardized product dimensions with business transformation",
    table_properties={
        "quality":"silver",
        "layer":"silver",
        "delta.enableChangeDataFeed":"true",
        "delta.autoOptimize.autoCompact":"true",
        "delta.autoOptimize.optimizeWrite":"true"
    }

)

def city_silver():
    df_bronze=spark.read.table("transportation.bronze.city")
    df_silver=df_bronze.select(
        F.col("city_id").alias("city_id"),
        F.col("city_name").alias("city_name"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp")
    )
    df_silver=df_silver.withColumn(
        "silver_processed_timestamp",F.current_timestamp()
    )

    return df_silver