package com.github.asolimando.mapspark

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

trait Helper {
  val WHLOCATION = "spark-warehouse"
  val LOCALDIR = "tmpdir"

  def generate_map(map_keys: IndexedSeq[String], rand:Random = new Random(1234)): Map[String, Option[Float]] =
    map_keys.map(k => (k, if(rand.nextFloat() > 0.5) Some(rand.nextFloat()) else None)).toMap

  def writeParquet(df: DataFrame, path: String): Unit = df.write.parquet(path)

  def readParquet(spark: SparkSession, path: String): DataFrame = spark.read.parquet(path)

  def init(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("SPARKMAP2017")
      .config("spark.sql.warehouse.dir", WHLOCATION)
      .config("spark.local.dir", LOCALDIR)
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }
}
