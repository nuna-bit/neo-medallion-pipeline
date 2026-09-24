from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.silver.neo_approaches import SILVER_DIR

GOLD_DIR = "data/gold"
SQL_DIR = Path(__file__).parent
GOLD_TABLES = ["daily_neo_counts", "weekly_neo_counts"]


def build_table(spark: SparkSession, silver: DataFrame, name: str) -> DataFrame:
    silver.createOrReplaceTempView("neo_approaches")
    return spark.sql((SQL_DIR / f"{name}.sql").read_text())


def run(spark: SparkSession) -> None:
    silver = spark.read.parquet(SILVER_DIR)
    for name in GOLD_TABLES:
        table = build_table(spark, silver, name)
        table.write.mode("overwrite").parquet(f"{GOLD_DIR}/{name}")
        print(f"\n{name}")
        table.show(truncate=False)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("neo-gold").getOrCreate()
    run(spark)
    spark.stop()
