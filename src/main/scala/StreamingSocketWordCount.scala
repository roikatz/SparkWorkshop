import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object StreamingSocketWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(line=>line.split(" ")).filter(s=> !s.isEmpty && s != null)
    val wordLengths = words.map(w => (w,w.length))

//    wordLengths.foreachRDD(rdd=> {
//      rdd.collect.foreach( word => {
//        println("********* Start Word *********")
 //       println(word._1 + ", Length: " + word._2)
//        println("*********  End Word *********")
//      })
//    })

    val tuples = words.map(w => (w,1))
    val count = tuples.reduceByKey(_+_)

//    count.foreachRDD(rdd=> {
//      rdd.collect.foreach( word => {
//                println("********* Start Word *********")
//        println(word._1 + ", Count: " + word._2)
//                println("*********  End Word *********")
//      })
//    })
//    val count = tuples.reduceByKeyAndWindow(_+_, Seconds(30))

    count.foreachRDD(rdd => {
      println("******************* Start Window *********************")
      rdd.collect().foreach(println)
      println("******************* End Window *********************")

    }   )

    ssc.start()
    ssc.awaitTermination()

  }
}
