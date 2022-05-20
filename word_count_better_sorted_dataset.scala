package Exercise
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, lower, split}
object word_count_better_sorted_dataset extends App {
  case class Book(value: String)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark=SparkSession
    .builder
    .master("local[*]")
    .appName("wordcountusingdataset")
    .getOrCreate()
  import spark.implicits._
  val readingBook=spark.read.text("data/book.txt").as[Book]
  val words = readingBook
    .select(explode(split($"value", "\\W+")).alias("word"))
    .filter($"word" =!= "")

  // Normalize everything to lowercase
  val lowercaseWords = words.select(lower($"word").alias("word"))

  // Count up the occurrences of each word
  val wordCounts = lowercaseWords.groupBy("word").count()

  // Sort by counts
  val wordCountsSorted = wordCounts.sort("count")

  // Show the results.
  wordCountsSorted.show(wordCountsSorted.count.toInt)

}
