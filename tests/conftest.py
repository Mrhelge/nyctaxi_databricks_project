import os
import sys
import pytest

sys.path.append(os.getcwd())

@pytest.fixture(scope="function")
def spark():
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
    except ImportError:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        except ImportError:
            raise ImportError("Neither Databricks Session nor Spark Session are available")
    return spark 