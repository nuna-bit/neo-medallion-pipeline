from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, MapType, StringType, StructField, StructType

BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver/neo_approaches"

# Only the fields silver needs. near_earth_objects is a map of "YYYY-MM-DD" -> list of asteroids.
BRONZE_SCHEMA = StructType([
    StructField("near_earth_objects", MapType(StringType(), ArrayType(StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("is_potentially_hazardous_asteroid", BooleanType()),
        StructField("close_approach_data", ArrayType(StructType([
            StructField("close_approach_date", StringType()),
            StructField("orbiting_body", StringType()),
        ]))),
    ])))),
])


def read_bronze(spark: SparkSession, bronze_dir: str = BRONZE_DIR) -> DataFrame:
    return (
        spark.read.schema(BRONZE_SCHEMA)
        .option("multiLine", True)
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.json")
        .json(bronze_dir)
    )


def transform(bronze: DataFrame) -> DataFrame:
    """One row per asteroid close approach to Earth, deduplicated across overlapping bronze files."""
    return (
        bronze
        .select(F.explode("near_earth_objects").alias("feed_date", "asteroids"))
        .select(F.explode("asteroids").alias("asteroid"))
        .select("asteroid.*")
        .select(
            F.col("id").alias("asteroid_id"),
            F.col("name").alias("asteroid_name"),
            F.col("is_potentially_hazardous_asteroid").alias("is_hazardous"),
            F.explode("close_approach_data").alias("approach"),
        )
        .where(F.col("approach.orbiting_body") == "Earth")
        .select(
            "asteroid_id",
            "asteroid_name",
            F.to_date("approach.close_approach_date").alias("approach_date"),
            "is_hazardous",
        )
        .where(F.col("asteroid_id").isNotNull() & F.col("approach_date").isNotNull())
        .dropDuplicates(["asteroid_id", "approach_date"])
    )


def run(spark: SparkSession) -> None:
    transform(read_bronze(spark)).write.mode("overwrite").parquet(SILVER_DIR)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("neo-silver").getOrCreate()
    run(spark)
    spark.stop()
