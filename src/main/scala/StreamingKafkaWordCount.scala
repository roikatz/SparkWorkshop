import _root_.kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._


object StreamingKafkaWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))

    val zkQuorum = "localhost:2181" // for consumer/receiver approach
    val topics = Map("sparkStreaming" -> 2) // for consumer approach - topic and parallelism level
    val topicsSet = Set("sparkStreaming")
    val brokers = Map("metadata.broker.list" -> "localhost:9092") // for direct approach
    val groupId = "spark"

    ssc.checkpoint("checkpoint") // For reduce by key and window

    //    val lines = KafkaUtils.createStream(ssc, zkQuorum,groupId, topics).map(_._2) // receiver based approach spark 1.2
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, brokers, topicsSet).map(_._2) // Direct approach - spark 1.3

    val words = lines.flatMap(line => line.split(" ")).filter(s => !s.isEmpty && s != null)
//    val wordCount = words.map(w => (w, 1)).reduceByKeyAndWindow( + _, _ + _, Minutes(10), Seconds(10), 2)
//    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
