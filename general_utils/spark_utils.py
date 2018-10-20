from pyspark.sql import SparkSession


def get_spark_context():
    spark = SparkSession.builder.appName("MySpark").getOrCreate()
    sc = spark.sparkContext
    return spark, sc