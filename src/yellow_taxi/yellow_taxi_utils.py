# this will be used to store functions specific to yellow-taxi project
import os
import sys
import requests
from time import sleep
from random import uniform

project_dir = os.path.abspath(os.path.join(os.getcwd(),"..","..","..",".."))
sys.path.append(project_dir)

from pyspark.sql.functions import col, max, lit, when, broadcast
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from src.helper_utils.functions import months_to_load, add_ingestion_metadata, return_validity_date, VALID_TO_DATE

MAX_RETRIES = 3
###LANDING###

def download_trips_data(output_dir, end_date):
    """
    Downloads the monthly yellow trips parquet files for a run date.

    The months come from `months_to_load(end_date)`. Files that already exist
    in `output_dir` are skipped, so rerunning with the same `end_date` does
    not download anything twice. Responses with status 429 or 503 are retried
    up to MAX_RETRIES times; any other error fails that month immediately.

    Parameters
    ----------
    output_dir : str
        Volume directory where the parquet files are saved.
    end_date : str
        Run date in "YYYY-MM-DD" format.

    Returns
    -------
    int
        Number of files downloaded in this run. 0 if every month was
        already on disk.

    Raises
    ------
    RuntimeError
        If any month could not be downloaded. The months that did download
        are kept, so the next run only retries the failed ones.
    """
    failed_period = []
    period_count = 0
    period_range = months_to_load(end_date)

    for period in period_range:
        file_name = f"yellow_tripdata_{period}.parquet"
        output_path = f"{output_dir}/{file_name}"

        if os.path.exists(output_path):
            print(f"File already exists, skipping {file_name}")
            continue

        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

        for retry in range(MAX_RETRIES):

            try:
                response = requests.get(url, timeout=60)
                response.raise_for_status()

                with open(output_path, "wb") as file:
                    file.write(response.content)
                    print(f"Downloaded file {file_name}")
                    period_count += 1
                    break
            except requests.exceptions.HTTPError as e:
                if response.status_code in (429, 503):
                    print(f"Error, will wait 30 sec and try again..")
                    sleep(30)
                else:
                    print(f"HTTP error {file_name}: {e}")
                    failed_period.append(period)
                    break
            except Exception as e:
                print(f"Error downloading file {file_name}: {e}")
                failed_period.append(period)
                break

            sleep(uniform(3,7))
        else:
            failed_period.append(period)
    if len(failed_period) > 0:
        raise RuntimeError(f"Failed to download files for periods: {failed_period}")
    return period_count

def download_taxi_lookup_data(local_file_name, output_dir):
    """
    Function that downloads the lookup data for nyxtaxi yellow trips from the official website
    """
    source_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    
    try:
        response = requests.get(source_url, timeout=60)
        response.raise_for_status()

        with open(os.path.join(output_dir, local_file_name), "wb") as file:
            file.write(response.content)
            print(f"Downloaded file {local_file_name}")
    except Exception as e:
        print(f"Failed to download file: {e}")

###BRONZE###
def ingest_raw_trips(spark: SparkSession, target_table: str, schema_location: str, source_location: str, checkpoint_location: str) -> None:
    """Ingests raw parquet trip data using Auto Loader (cloudFiles) and appends
    metadata columns before streaming to a Delta table.

    Runs with an availableNow trigger: processes all new files, then stops.
    Blocks until the stream finishes.

    Parameters
    ----------
    spark : SparkSession
        Active Spark Session.
    target_table : str
        Target table name to write stream data into.
    schema_location : str
        Cloud storage path for Auto Loader schema inference/evolution metadata.
    source_location : str
        Cloud storage location containing the source files.
    checkpoint_location : str
        Cloud storage path for stream checkpointing.

    Returns
    -------
    None
    """
    df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(source_location)
        )

    df_meta = add_ingestion_metadata(df)

    query = (df_meta.writeStream
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
        .toTable(target_table)
        )

    query.awaitTermination()

def ingest_lookup_data(spark: SparkSession, source_location: str, target_table: str) -> None:
    """
    Loads a lookup CSV into a bronze table, appending only when the file is new.

    Adds ingestion metadata columns. If the target table does not exist, the
    data is written to create it. Otherwise the data is appended only when the
    source file was modified after the latest `_source_file_last_modified`
    already in the target, so an unchanged file is not loaded twice.

    Parameters
    ----------
    spark : SparkSession
        Active Spark Session.
    source_location : str
        Path to the lookup CSV file(s).
    target_table : str
        Name of the bronze table to write to.

    Returns
    -------
    None
    """
    source_df = spark.read.format("csv").option("header", True).load(source_location)
    df_with_metadata = add_ingestion_metadata(source_df)

    if not spark.catalog.tableExists(target_table):
        df_with_metadata.write.mode("append").saveAsTable(target_table)
    else:
        source_row = df_with_metadata.select(max(col("_metadata.file_modification_time")).alias("last_modified")).first() # find file modif date

        target_row = spark.read.table(target_table).select(max(col("_source_file_last_modified")).alias('last_modified')).first()

        if source_row["last_modified"] > target_row["last_modified"]:
            df_with_metadata.write.mode("append").saveAsTable(target_table)

###SILVER###
def latest_lookup_snapshot(lookup_df: DataFrame) -> DataFrame:
    """
    Keeps only the rows from the most recent ingestion of a lookup table.

    Parameters
    ----------
    lookup_df : DataFrame
        Bronze lookup DataFrame with an `_ingestion_timestamp` column.

    Returns
    -------
    DataFrame
        Rows whose `_ingestion_timestamp` equals the latest one.
    """
    max_ingestion_ts = (
        lookup_df.agg(max("_ingestion_timestamp")).first()[0]
    )
    return (
    lookup_df.filter(col("_ingestion_timestamp") == max_ingestion_ts)
)

def normalize_lookup(source_df: DataFrame) -> DataFrame:
    """
    Renames and casts the taxi zone lookup columns to the silver schema.

    Parameters
    ----------
    source_df : DataFrame
        Raw taxi zone lookup DataFrame with `LocationID`, `Borough`, `Zone`
        and `service_zone` columns.

    Returns
    -------
    DataFrame
        DataFrame with `location_id` (int), `borough`, `zone` and
        `service_zone` (string) columns only.
    """
    return  (
    source_df
    .select(
        col("LocationID").alias("location_id").cast("int"),
        col("Borough").alias("borough").cast("string"),
        col("Zone").alias("zone").cast("string"),
        col("service_zone").cast("string")
    )
)

def identify_scd2_changes(source_df: DataFrame, target_df: DataFrame) -> DataFrame:
    """
    Compares source and target zone rows by hash and labels each change.

    Full outer join on `location_id`, then sets `change_type`:
    "update" when both hashes exist and differ, "insert" when the location is
    only in the source, "delete" when it is only in the target. Unchanged
    rows are dropped.

    Parameters
    ----------
    source_df : DataFrame
        Incoming zone data with `location_id`, `borough`, `zone`,
        `service_zone` and an `s_hash` column.
    target_df : DataFrame
        Current rows of the SCD2 table with `location_id` and a `t_hash` column.

    Returns
    -------
    DataFrame
        Changed rows with `location_id`, source `borough`, `zone`,
        `service_zone` (null for deletes) and `change_type`.
    """
    return (source_df.alias("s")
           .join(target_df.alias("t"), on="location_id", how="outer")
           .withColumn("change_type",
                        when(col("s_hash") != col("t_hash"), lit("update")).
                        when(col("t_hash").isNull(), lit("insert")).
                        when(col("s_hash").isNull(), lit("delete")).
                        otherwise(lit("no change"))
                        )
            .filter(col("change_type") != "no change")
            .select("location_id", "s.borough", "s.zone", "s.service_zone", "change_type")
            )

def rows_to_close(df_with_changes: DataFrame) -> DataFrame:
    """
    Selects the changes that must close the current SCD2 version.

    Keeps "update" and "delete" rows and sets `merge_key` to `location_id`,
    so they match the current target row during the merge.

    Parameters
    ----------
    df_with_changes : DataFrame
        Output of `identify_scd2_changes`.

    Returns
    -------
    DataFrame
        Rows to close, with a `merge_key` column added.
    """
    return (df_with_changes
                .filter(col("change_type").isin('update', 'delete'))
                .withColumn("merge_key", col("location_id"))
            )

def rows_to_insert(df_with_changes: DataFrame) -> DataFrame:
    """
    Selects the changes that must insert a new SCD2 version.

    Keeps "insert" and "update" rows and sets `merge_key` to null, so they
    never match a target row and are always inserted during the merge.

    Parameters
    ----------
    df_with_changes : DataFrame
        Output of `identify_scd2_changes`.

    Returns
    -------
    DataFrame
        Rows to insert, with a null `merge_key` column added.
    """
    return (df_with_changes
                .filter(col("change_type").isin('update', 'insert'))
                .withColumn("merge_key", lit(None).cast("int"))
                )

def run_scd2(target_table: DeltaTable, changed_rows: DataFrame, run_date: str) -> None:
    """
    Applies SCD Type 2 changes to the zone dimension with a single MERGE.

    Rows whose `merge_key` matches a current target row close that row:
    `valid_to` is set to the change date and `is_current` to False.
    Rows that do not match and have change_type "insert" or "update" are
    inserted as the new current version, valid from the change date to
    VALID_TO_DATE. The change date comes from `return_validity_date()`.

    Parameters
    ----------
    target_table : DeltaTable
        The SCD2 zone table.
    changed_rows : DataFrame
        Union of `rows_to_close` and `rows_to_insert` output.
    run_date : str
        The date to use for calculating validity timestamps.

    Returns
    -------
    None
    """

    VALID_TS = lit(return_validity_date(run_date)).cast("timestamp")

    (target_table.alias("t")
    .merge(changed_rows.alias("s"), "t.location_id = s.merge_key and t.is_current == True")
    .whenMatchedUpdate(set=
                {
                    "t.valid_to": VALID_TS,
                    "t.is_current": lit(False)
                })
    .whenNotMatchedInsert(
                        condition = "s.change_type in ('insert', 'update')",
                        values = {
                            "t.location_id" : "s.location_id",
                            "t.borough" : "s.borough",
                            "t.zone" : "s.zone",
                            "t.service_zone" : "s.service_zone",
                            "t.valid_from" : VALID_TS,
                            "t.valid_to" : lit(VALID_TO_DATE).cast("timestamp"),
                            "t.is_current" : lit(True)
                    }
        )
    .execute()
)



def normalize_yellow_trips(df: DataFrame, min_pickup_timestamp: str, max_pickup_timestamp: str) -> DataFrame:
    """
    Renames and casts raw yellow trip columns and filters by pickup time.

    Converts column names to snake_case, casts them to their silver types,
    keeps the ingestion metadata columns, and drops rows with a null pickup or
    dropoff time or a pickup time outside the given range.

    Parameters
    ----------
    df : DataFrame
        Bronze yellow trips DataFrame.
    min_pickup_timestamp : str
        Lower bound for `pickup_datetime`, inclusive (e.g. "2025-04-01").
    max_pickup_timestamp : str
        Upper bound for `pickup_datetime`, exclusive (e.g. "2026-04-01").

    Returns
    -------
    DataFrame
        Normalized and filtered trips DataFrame.
    """
    return (df
      .select(
          col("VendorID").cast("int").alias("vendor_id"),
          col("tpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
          col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
          col("passenger_count").cast("int"),
          col("trip_distance").cast("double"),
          col("RatecodeID").cast("int").alias("rate_code_id"),
          col("PULocationID").cast("int").alias("pickup_location_id"),
          col("DOLocationID").cast("int").alias("dropoff_location_id"),
          col("payment_type").cast("int"),
          col("fare_amount").cast("double"),
          col("extra").cast("double"),
          col("mta_tax").cast("double"),
          col("tip_amount").cast("double"),
          col("tolls_amount").cast("double"),
          col("improvement_surcharge").cast("double"),
          col("total_amount").cast("double"),
          col("congestion_surcharge").cast("double"),
          col("Airport_fee").cast("double").alias("airport_fee"),
          col("cbd_congestion_fee").cast("double"),
          col("_ingestion_timestamp").cast("timestamp"),
          col("_source_file_name").cast("string"),
          col("_source_file_path").cast("string"),
          col("_source_file_last_modified").cast("timestamp")
        )
    .filter(col("pickup_datetime").isNotNull())
    .filter(col("dropoff_datetime").isNotNull())
    .filter((col("pickup_datetime") >= min_pickup_timestamp) & (col("pickup_datetime") < max_pickup_timestamp))
)

def merge_insert(df_to_merge: DataFrame, target_table: DeltaTable) -> None:
    """
    Inserts rows whose `trip_id` is not already in the target table.

    Existing trips are left unchanged, so re-running the load does not
    create duplicates.

    Parameters
    ----------
    df_to_merge : DataFrame
        Trips to load, with a `trip_id` column and the target table's schema.
    target_table : DeltaTable
        The target trips table.

    Returns
    -------
    None
    """
    (target_table.alias("t")
     .merge(df_to_merge.alias("s"), "t.trip_id = s.trip_id")
     .whenNotMatchedInsertAll()
     .execute()
    )

def enrich_yellow_trips(silver_trips: DataFrame, vendor: DataFrame, payment_type: DataFrame, rates: DataFrame, silver_lookup: DataFrame) -> DataFrame:
    """
    Joins silver trips with lookup tables to replace codes with descriptions.

    All joins are broadcast left joins, so trips without a match are kept
    with null descriptions. Pickup and dropoff zones are joined to the SCD2
    zone table using the version valid at the pickup / dropoff time
    (`valid_from` <= datetime < `valid_to`).

    Parameters
    ----------
    silver_trips : DataFrame
        Normalized trips with `trip_id`, code columns and fare columns.
    vendor : DataFrame
        Vendor lookup with `vendor_id` and `description`.
    payment_type : DataFrame
        Payment type lookup with `payment_type` and `description`.
    rates : DataFrame
        Rate code lookup with `rate_code_id` and `description`.
    silver_lookup : DataFrame
        SCD2 zone table with `location_id`, `borough`, `zone`,
        `service_zone`, `valid_from` and `valid_to`.

    Returns
    -------
    DataFrame
        Enriched trips with vendor, payment method, rate name and pickup /
        dropoff borough, zone and service zone, plus fares and ingestion
        metadata.
    """
    return (silver_trips.alias("t")
                    .join(broadcast(vendor.alias("v")), on="vendor_id", how="left")
                    .join(broadcast(payment_type.alias("p")), on="payment_type", how="left")
                    .join(broadcast(rates.alias("r")), on="rate_code_id", how="left")
                    .join(broadcast(silver_lookup.alias("pl")), on=(col("t.pickup_location_id") == col("pl.location_id")) & (col("t.pickup_datetime") >= col("pl.valid_from")) & (col("t.pickup_datetime") < col("pl.valid_to")), how='left')
                    .join(broadcast(silver_lookup.alias("dl")), on=(col("t.dropoff_location_id") == col("dl.location_id")) & (col("t.dropoff_datetime") >= col("dl.valid_from")) & (col("t.dropoff_datetime") < col("dl.valid_to")), how='left')
                    .select(
                            col("t.trip_id"),
                            col("v.description").alias("vendor"),
                            col("t.pickup_datetime"),
                            col("t.dropoff_datetime"),
                            col("pl.borough").alias("pickup_borough"),
                            col("pl.zone").alias("pickup_zone"),
                            col("pl.service_zone").alias("pickup_service_zone"),
                            col("dl.borough").alias("dropoff_borough"),
                            col("dl.zone").alias("dropoff_zone"),
                            col("dl.service_zone").alias("dropoff_service_zone"),
                            col("t.passenger_count"),
                            col("t.trip_distance"),
                            col("t.fare_amount"),
                            col("t.extra"),
                            col("t.mta_tax"),
                            col("t.tip_amount"),
                            col("t.tolls_amount"),
                            col("t.improvement_surcharge"),
                            col("t.total_amount"),
                            col("p.description").alias("payment_method"),
                            col("r.description").alias("rate_name"),
                            col("t.congestion_surcharge"),
                            col("t.airport_fee"),
                            col("t.cbd_congestion_fee"),
                            col("t._ingestion_timestamp"),
                            col("t._source_file_name"),
                            col("t._source_file_path"),
                            col("t._source_file_last_modified")
                            )
            )
