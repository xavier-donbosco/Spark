package Exercise
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
object fake_friends_dataset extends App {
  case class FakeFriends(id: Int, name: String,age: Int, friends : Long)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark=SparkSession
    .builder
    .master("local[*]")
    .appName("fakefriendsdataset")
    .getOrCreate()
  import spark.implicits._
  val fakeF=spark.read
    .option("header","true")
    .option("inferSchema","true")
    .csv("data/fakefriends.csv")
    .as[FakeFriends]

  val selectStatement=fakeF.select(("name"),("friends"))
  selectStatement.groupBy("name").avg("friends").sort("name").show()




}
