//import com.couchbase.client.java.document.{JsonDocument, JsonArrayDocument}
//import com.couchbase.client.java.document.json.{JsonObject, JsonArray}

import _root_.kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import com.couchbase.spark.streaming._

import com.couchbase.client.java.document.{JsonDocument, JsonArrayDocument}
import com.couchbase.client.java.document.json.{JsonObject, JsonArray}


object KafkaToCouchbase {
  def main (args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("KafkaWordCount")
      .setMaster("local[2]")
      .set("com.couchbase.nodes", "localhost")
      .set("com.couchbase.bucket.SparkDemo", "")

    val window = Seconds(5)

    val ssc = new StreamingContext(conf, window)

    val topicsSet = Set("sparkStreaming")
    val brokers = Map("metadata.broker.list" -> "localhost:9092") // for direct approach
    val groupId = "spark"

    ssc.checkpoint("checkpoint") // For reduce by key and window

    //    val lines = KafkaUtils.createStream(ssc, zkQuorum,groupId, topics).map(_._2) // receiver based approach spark 1.2
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, brokers, topicsSet).map(_._2) // Direct approach - spark 1.3

    val words = lines.flatMap(line => line.split(" ")).filter(s => !s.isEmpty && s != null)
    val wordCount = words.map(w => (w, 1)).reduceByKeyAndWindow((a:Int, b:Int)=>a+b, window, window)
    val jsonMap = wordCount.map(
      tuple => {
        val id = "_tags:: " + System.currentTimeMillis()
        val content = JsonArray.create()
        content.add(JsonObject.create().put("word", tuple._1).put("count", tuple._2))
        println(id, content)
        JsonArrayDocument.create(id, content)
      }).saveToCouchbase("SparkDemo")


    ssc.start()
    ssc.awaitTermination()
  }

}
