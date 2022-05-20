package Exercise

import org.apache.spark.sql._
import org.apache.log4j._

object Spark_Sql {
  case class Person(ID: Int, name: String, age: Int, numFriends: Int)

  def mapper(line: String): Person = {
    val field = line.split(",")

    val person:Person = Person(field(0).toInt,field(1),field(2).toInt,field(3).toInt)

    person
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("sparksql")
      .master("local[*]")
      .getOrCreate()
    val input = spark.sparkContext.textFile("data/fakefriends.csv")

    val people = input.map(mapper)

    import spark.implicits._
    val peopleSchema = people.toDS()
    peopleSchema.printSchema()

    peopleSchema.createOrReplaceTempView("people")

    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    val result = teenagers.collect()
    result.foreach(println)
    spark.stop()
  }
}
