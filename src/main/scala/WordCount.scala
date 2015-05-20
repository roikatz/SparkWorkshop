import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object WordCount{
  def main ( args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val files = sc.textFile("/Users/roikatz/Documents/Spark/Data/books/The Complete Works of William Shakespeare by William Shakespeare.txt")
    val words = files.flatMap(line=>line.split(" ")).filter(s=> !s.isEmpty && s != null)

    val tuples = words.map(w => (w,1))
    val count = tuples.reduceByKey(_+_)

    val sorted = count.map(t=>(t._2,t._1)).sortByKey(false)

    sorted.take(15).foreach(println)
    println(sc.version)
  }
}
