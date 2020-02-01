package com.github.asolimando.mapspark

import org.apache.spark.sql.functions._

object MAPSpark extends Helper {
  val NUM_VALUES: Int = 10
  val NUM_LINES: Int = 10

  def main(args: Array[String]) {
    val spark = init()

    // generate some data and turn it into a df
    val map_keys = Range.apply(0, NUM_VALUES).map("value_%d".format(_))
    val data = Range.apply(0, NUM_LINES).map((_, generate_map(map_keys)))

    import spark.sqlContext.implicits._

    var df = spark.sparkContext.parallelize(data)
      .toDF("id", "values")

    df.printSchema()
    df.orderBy(col("id")).show(5, truncate = false)


    df = df.select(col("id"), explode(col("values")))
      .orderBy(col("id"))

    df.printSchema()
    df.show(50, truncate = false)


    df = df.filter(col("value").isNotNull)
      .select(col("key"))
      .distinct()

    df.printSchema()
    df.show(50, truncate = false)
    print(df.count())

    df = df.groupBy().agg(collect_list("key").as("active_keys"))
    df.printSchema()
    df.show(50, truncate = false)
  }
}
