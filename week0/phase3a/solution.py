"""
Phase 3a — PySpark / ETL (template — fill in)

Run: python solution.py  or  spark-submit solution.py
Notebook: reuse your SparkSession as `spark` and adapt below.
"""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("phase3a").getOrCreate()
    try:
        # TODO: extract — read sources (CSV / JSON / Parquet, etc.)
        # TODO: transform — clean, join, aggregate
        # TODO: load — write or .show() results
        pass
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
