package Exercise
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
object data_frames_dataset extends App {
  case class Person(ID: Int,name: String, age: Int,friends: Int )
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark=SparkSession
    .builder
    .master("local[*]")
    .appName("dataframes")
    .getOrCreate()
  import spark.implicits._
  val people=spark.read
    .option("header","true")
    .option("inferschema","true")
    .csv("data/fakefriends.csv")
    .as[Person]

  people.printSchema()
  people.select("name").show()
  people.select("name").filter(people("age")>21).show()
  people.select(("age"),("name")).filter(people("age")>=18).orderBy("name")
  spark.stop()
}
