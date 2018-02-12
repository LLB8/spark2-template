package sql_practice

import spark_helpers.SparkSessionHelper
import org.apache.spark.sql.functions._

object examples {
  def exec1(): Unit= {
    val spark = SparkSessionHelper.getSparkSession()
    var tourDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    tourDF.show

    // Q1 - How many unique levels of difficulties ?
    tourDF.groupBy("tourDifficulty")
      .count()
      .show

    // Q2 - What is min/max/avg of the prices ?
    tourDF.agg(avg("tourPrice"), min("tourPrice"), max("tourPrice"))
      .show()

    // Q3 - What is min/max/avg of the prices for each level of difficulty
    tourDF.groupBy("tourDifficulty")
      .agg(avg("tourPrice"), min("tourPrice"), max("tourPrice"))
      .show()

    // Q4 - What is min/max/avg of the prices and min/max/avg of the length for each level of difficulty
    tourDF.groupBy("tourDifficulty")
      .agg(avg("tourPrice"), min("tourPrice"), max("tourPrice"), avg("tourLength"), min("tourLength"), max("tourLength"))
      .show()

    // Q5 - Display the top 10 "tourTags"
    tourDF.select(explode(tourDF("tourTags")))
      .groupBy("col")
      .count()
      .orderBy(desc("count"))
      .show(10)

    // Q6 - Relationship between top 10 "tourTags" and "tourDifficulties
    tourDF.select(explode(tourDF("tourTags")),tourDF("tourDifficulty"))
      .groupBy("col","tourDifficulty")
      .count()
      .orderBy(desc("count"))
      .show(10)

    // Q7 - Add prices min/max/avg
    tourDF.select(explode(tourDF("tourTags")),tourDF("tourDifficulty"),tourDF("tourPrice"))
      .groupBy("col","tourDifficulty")
      .agg(avg("tourPrice"), min("tourPrice"), max("tourPrice"))
      .orderBy(desc("avg(tourPrice)"))
      .show(10)
  }
}
