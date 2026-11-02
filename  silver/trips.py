from pyspark import pipelines as dp
from pyspark.sql import functions as F


#staging view, temporary/intermediate view
@dp.view(
    name="trips_silver_staging",
    comment="Transformed trips data ready for CDC upsert"
)

#Expectation decorators
@dp.expect("valid_date","year(business_date) >=2020  AND business_date IS NOT NULL")   #if we get error, that will be logged
@dp.expect("valid_driver_rating","driver_rating BETWEEN 1 AND 10")
@dp.expect("valid_passenger_rating","passenger_rating BETWEEN 1 AND 10")

def trips_silver():
    #reading only updated records from bronze
    df_bronze=spark.readStream.table("transportation.bronze.trips")

    df_silver=df_bronze.select(
        F.col("trip_id").alias("id"),
        F.col("date").cast("date").alias("business_date"),
        F.col("city_id").alias("city_id"),
        F.col("passenger_type").alias("passenger_category"),
        F.col("distance_travelled_km").alias("distance_kms"),
        F.col("fare_amount").alias("sales_amount"),
        F.col("passenger_rating").alias("passenger_rating"),
        F.col("driver_rating").alias("driver_rating"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp")
    )

    df_silver=df_silver.withColumn(
        "silver_processed_timestamp",F.current_timestamp()
    )

    return df_silver

dp.create_streaming_table(
    name="transportation.silver.trips",
    comment="cleaned and validated orders with CDC upsert capability",
    table_properties={
        "quality":"silver",
        "layer":"silver",
        "delta.enableChangeDataFeed":"true",
        "delta.autoOptimize.optimizeWrite":"true",
        "delta.autoOptimize.autoCompact":"true"
    },
)

#now we want data to flow from silver staging layer to the silver streaming table
dp.create_auto_cdc_flow(
    target="transportation.silver.trips",
    source="trips_silver_staging",
    keys=["id"], #in the target table, if there is a record with the same trip id, it will be updated. UPSERT operation
    sequence_by=F.col("silver_processed_timestamp"), #tackles out of sequence record issue
    stored_as_scd_type=1, #old value replaced by the new value for a record (overwrite)
    except_column_list=[],

)





