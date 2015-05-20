import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object MapValues {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Map Values").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val seq  = Seq(("Test1", 99), ("Test2", 86), ("Test3",76))
    val rdd1 = sc.makeRDD(seq) // or makeRDD

    val values = rdd1.mapValues(testResult => testResult-10)
    values.foreach(println)

  }
}
