import fire
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pathlib import Path


def csv_to_parquet(source_csv_file: str, target_csv_file: str) -> None:
    spark = (SparkSession.builder
             .appName('csv_to_parquet')
             .getOrCreate()
             )

    # Extract
    df = spark.read.option("InferSchema", True).csv(
        source_csv_file,
        sep=',',
        header=True
    )

    df.show()

    (df.select(f.avg("call_time"))
     .write
     .mode("overwrite")
     .format("csv")
     .save(target_csv_file)
     )

    spark.stop()


if __name__ == '__main__':
    fire.Fire(csv_to_parquet)
