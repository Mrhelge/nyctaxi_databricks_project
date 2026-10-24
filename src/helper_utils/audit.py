from pyspark.sql.functions import col, max, sum, lit,when, coalesce

def describe_history(spark, table):
    return spark.sql(f"DESCRIBE HISTORY {table} ")

def get_table_version(spark, table):
    if not spark.catalog.tableExists(table):
        return None
    else:
        history = describe_history(spark, table)
        return history.agg(max("version")).first()[0]

def get_write_metrics(spark, table, version_before):

    if version_before is None:
        version_before = -1

    metrics = col("operationMetrics")
    inserted_rows = metrics["numTargetRowsInserted"].cast("bigint")
    updated_rows = metrics["numTargetRowsUpdated"].cast("bigint")
    output_rows = metrics["numOutputRows"].cast("bigint")
    read_rows = metrics["numSourceRows"].cast("bigint")


    if not spark.catalog.tableExists(table):
        return 0, 0, None
    else:
        history = describe_history(spark, table)
        changes = history.filter(col("version") > version_before)
        row = (changes
                     .agg(
                         coalesce(
                            sum(when(col("operation") == 'MERGE', read_rows).otherwise(output_rows)),
                         lit(0)
                         ).alias("rows_read"),
                         coalesce(
                             sum(when(col("operation") == 'MERGE', inserted_rows + updated_rows).otherwise(output_rows)),
                         lit(0)
                             ).alias("rows_written"),
                         max("version").alias("version")
                     )
                    ).first()
        return row["rows_read"], row["rows_written"], row["version"]