package Exercise
import Exercise.Popular_movie_dataset.Movie
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}
object popular_movies_nicer_dataset extends App {

  final case class Movie(userId: Int, movieId: Int, rating: Int, someother: Long)
  implicit val codec: Codec = Codec("ISO-8859-1")

  val lines=Source.fromFile("data/ml-100k/u.item")
  var movieLines:Map[Int,String]=Map()
  for(line <- lines.getLines()) {
    val fields=line.split("|")
    if(fields.length>1) {
      movieLines += (fields(0).toInt -> fields(1))
    }
  lines.close()
  }
  movieLines
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

  val topratedmovies=movieDs.groupBy("movieID").count()

  spark.stop()
}
