package Exercise
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
object Popular_movie_dataset extends App {
  final case class Movie(movieId: Int)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark=SparkSession
    .builder
    .appName("popular_movie")
    .master("local[*]")
    .getOrCreate()

  val movieSchema=new StructType()
    .add("userId",IntegerType,nullable = true)
    .add("movieId",IntegerType,nullable = true)
    .add("rating",IntegerType,nullable = true)
    .add("someother",LongType,nullable = true)

  import spark.implicits._

  val movieDs=spark.read
    .option("sep","\t")
    .schema(movieSchema)
    .csv("data/ml-100k/u.data")
    .as[Movie]

  val topratedmovies=movieDs.groupBy("movieID").count().orderBy(desc("count")).show(10)

  spark.stop()
}
