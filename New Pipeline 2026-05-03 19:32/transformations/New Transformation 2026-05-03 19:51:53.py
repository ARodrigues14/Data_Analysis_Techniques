from pyspark import pipelines as dp


@dp.table
def new_transformation_2026_05_03_19_51_53():
    return spark.read.table("consumption_bronze")