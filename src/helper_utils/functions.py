from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, md5, concat_ws, coalesce, row_number, max
from pyspark.sql import Window
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

VALID_FROM_DATE = "2015-01-01 00:00:00"
VALID_TO_DATE = "9999-12-31 00:00:00"
PIPELINE_START_DATE = "2025-04-01"
LAG_MONTHS = 3

def months_to_load(end_date, lag_months=LAG_MONTHS, start_date=PIPELINE_START_DATE):
    """
    Returns the months of trip data that should be available for a given run date.

    Trip files are published with a delay, so the last month to load is the
    month of `end_date` minus `lag_months`. The first month is `start_date`.
    Only the year and month of `end_date` are used, so every day of the same
    month gives the same result.

    For example, with end_date "2026-10-07", lag_months 3 and start_date
    "2025-04-01" this returns ["2025-04", "2025-05", ..., "2026-07"].

    Parameters
    ----------
    end_date : str
        Run date in "YYYY-MM-DD" format.
    lag_months : int
        Number of months between the run date and the last month to load.
    start_date : str
        First month to load, in "YYYY-MM-DD" format. Must be the first day
        of a month, otherwise that month is skipped.

    Returns
    -------
    list[str]
        Months to load in "YYYY-MM" format, oldest first. Empty if the last
        month to load is earlier than `start_date`.
    """
    parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d")
    end_period = (parsed_end_date - relativedelta(months=lag_months)).replace(day=1).strftime("%Y-%m-%d")

    date_range = pd.date_range(start=start_date, end=end_period, freq='MS')
    return [d.strftime("%Y-%m") for d in date_range]




def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """
    Adds ingestion audit columns to a DataFrame read from files.

    Adds `_ingestion_timestamp` (current timestamp) and the source file's
    name, path and last-modified time taken from the hidden `_metadata` column.

    Parameters
    ----------
    df : DataFrame
        DataFrame read from a file-based source (batch or streaming),
        so that the `_metadata` column is available.

    Returns
    -------
    DataFrame
        The input DataFrame with `_ingestion_timestamp`, `_source_file_name`,
        `_source_file_path` and `_source_file_last_modified` columns added.
    """
    return (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file_name", col("_metadata.file_name"))
        .withColumn("_source_file_path", col("_metadata.file_path"))
        .withColumn("_source_file_last_modified", col("_metadata.file_modification_time"))
        )

def add_initial_scd2_colummns(df: DataFrame) -> DataFrame:
    """
    Adds SCD Type 2 validity columns for the initial load of a dimension.

    Every row is marked as the current version, valid from VALID_FROM_DATE
    to VALID_TO_DATE.

    Parameters
    ----------
    df : DataFrame
        The input PySpark DataFrame.

    Returns
    -------
    DataFrame
        The input DataFrame with `valid_from` (timestamp), `valid_to`
        (timestamp) and `is_current` (True) columns added.
    """
    return (
        df
        .withColumns(
            {
                "valid_from": lit(VALID_FROM_DATE).cast("timestamp"),
                "valid_to": lit(VALID_TO_DATE).cast("timestamp"),
                "is_current": lit(True)
            }
        )
    )


def add_md5_hash_id(df: DataFrame, hash_col_name: str, cols: list[str]) -> DataFrame:
    """
    Generates an MD5 hash column based on a list of input columns.

    Null values are replaced with "0" and the values are joined with "||"
    before hashing, so the same inputs always produce the same hash. Used to
    detect changes in the tracked columns of an SCD2 dimension.

    Parameters
    ----------
    df : DataFrame
        The input PySpark DataFrame.
    hash_col_name : str
        The name of the new output column to create.
    cols : list[str]
        Names of the columns to include in the hash (the SCD2 tracked columns).

    Returns
    -------
    DataFrame
        A new DataFrame containing the added MD5 hash column (hex string).
    """
    return (
        df
        .withColumn(hash_col_name, md5(concat_ws("||", *[coalesce(c, lit("0")) for c in cols]))
    )
    )

def deduplicate(df: DataFrame, partition_by_cols: list[str], order_by_cols: list[str]) -> DataFrame:
    """
    Deduplicates a DataFrame by partitioning over specified columns and keeping
    the top record for each group based on a descending sort order.

    Parameters
    ----------
    df : DataFrame
        The input PySpark DataFrame.
    partition_by_cols : list[str]
        List of column names used to group/partition records for deduplication.
        A single column name as str is also accepted.
    order_by_cols : list[str]
        List of column names used to rank records within each partition in descending order.
        The row with the highest values across these columns is retained.
        A single column name as str is also accepted.

    Returns
    -------
    DataFrame
        A new DataFrame containing only the top record per partition, with
        the temporary ranking column removed.
    """
    if isinstance(partition_by_cols, str):
        partition_by_cols = [partition_by_cols]

    if isinstance(order_by_cols, str):
        order_by_cols = [order_by_cols]

    sort_desc = [col(c).desc() for c in order_by_cols]
    w = Window.partitionBy(*partition_by_cols).orderBy(*sort_desc)

    return (
        df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

def get_incremental_data(spark: SparkSession, target_table: str, source_df: DataFrame) -> DataFrame:
    """
    Returns only the source rows that have not been loaded to the target yet.

    If the target table exists, keeps source rows whose `_ingestion_timestamp`
    is later than the latest `_ingestion_timestamp` in the target. If the
    target table does not exist, returns the full source DataFrame.

    Parameters
    ----------
    spark : SparkSession
        Active Spark Session.
    target_table : str
        Name of the target table.
    source_df : DataFrame
        Source DataFrame with an `_ingestion_timestamp` column.

    Returns
    -------
    DataFrame
        Source rows that are new relative to the target table.
    """

    if spark.catalog.tableExists(target_table):
        silver_trips_normalized = spark.table(target_table)
        max_ingestion_ts_target = silver_trips_normalized.agg(max(col("_ingestion_timestamp"))).first()[0]
        return (
            source_df.filter(col("_ingestion_timestamp") > max_ingestion_ts_target)
            )
    else:
        return source_df

def return_validity_date(run_date: str, lag_months=LAG_MONTHS) -> str:
    """
    Returns the SCD2 change date: the first day of the month three months ago.

    For example, when run in September 2026 this evaluates to 2026-06-01 00:00:00.
    Parameters
    ----------
    run_date : str
        The date to calculate the validity timestamp for.
    lag_months : int, optional
        The number of months to subtract from the run date, by default LAG_MONTHS

    Returns
    -------
    String
        Timestamp column expression, evaluated when the query runs.
    """

    parsed_date = datetime.strptime(run_date, "%Y-%m-%d")
    validity_date = (parsed_date - relativedelta(months=lag_months)).replace(day=1).strftime("%Y-%m-%d")

    return validity_date

def return_pipeline_upper_bound(run_date:str, lag_months: int = LAG_MONTHS) -> str:
    """
    Returns upper bound of the pipeline, which is the first day of the month after run_date minus lag_months. For example, if run_date is 2026-10-07 and lag_months is 3, this returns 2026-08-01.
    """
    parsed_date = datetime.strptime(run_date, "%Y-%m-%d")

    last_day = (parsed_date - relativedelta(months=lag_months-1)).replace(day=1).strftime("%Y-%m-%d")
    return last_day
