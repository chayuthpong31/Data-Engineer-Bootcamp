from chispa.dataframe_comparer import *
from ..jobs.actors_scd_job import do_actor_scd_transformation
from collections import namedtuple
from pyspark.sql.types import LongType
from pyspark.sql.functions import col

ActorYear = namedtuple("ActorYear", "actor actorid current_year quality_class is_active")
ActorScd = namedtuple("ActorScd", "actor actorid quality_class is_active start_year end_year current_year")


def test_scd_generation(spark):
    source_data = [
        ActorYear("Meat Loaf","101", 2018, 'Good', True),
        ActorYear("Meat Loaf","101", 2019, 'Good', True),
        ActorYear("Meat Loaf","101", 2020, 'Bad', True),
        ActorYear("Meat Loaf","101", 2021, 'Bad', True),
        ActorYear("Skid Markel","102", 2020, 'Bad', True),
        ActorYear("Skid Markel","102", 2021, 'Bad', True)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_scd_transformation(spark, source_df)
    expected_data = [
        ActorScd("Meat Loaf","101", 'Good', True, 2018, 2019, 2020),
        ActorScd("Meat Loaf","101", 'Bad', True, 2020, 2021, 2020),
        ActorScd("Skid Markel","102", 'Bad', True, 2020, 2021, 2020)
    ]
    expected_df = spark.createDataFrame(expected_data)
    actual_df = actual_df.withColumn("current_year", col("current_year").cast(LongType()))
    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_metadata=True)