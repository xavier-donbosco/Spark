import org.apache.spark._
import org.apache.log4j._
object WordCountBetterSorted {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local", "WordCountBetterSorted")   
    val input = sc.textFile("data/book.txt")
    val words = input.flatMap(x => x.split("\\W+"))
    val lowercaseWords = words.map(x => x.toLowerCase())
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
    
  }
  
}

