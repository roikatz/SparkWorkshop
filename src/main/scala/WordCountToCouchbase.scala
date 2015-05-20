import org.apache.spark.{SparkContext, SparkConf}
import com.couchbase.spark._
import com.couchbase.client.java.document.{JsonDocument, JsonArrayDocument}
import com.couchbase.client.java.document.json.{JsonObject, JsonArray}


object WordCountToCouchbase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountToCouchbase")
      .setMaster("local[*]")
      .set("com.couchbase.nodes", "localhost")
      .set("com.couchbase.bucket.SparkDemo", "")

    val sc = new SparkContext(conf)

//    sc.parallelize(0 until 100)
//      .map(i => JsonDocument.create("doc-" + i, JsonObject.create().put("number", i)))
//      .saveToCouchbase()


    val files = sc.textFile("/Users/roikatz/Documents/Spark/Data/books/The Complete Works of William Shakespeare by William Shakespeare.txt")

    val wordCount =
      files.flatMap(line => line.split(" ")).filter(s => !s.isEmpty && s != null)
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .map(t => (t._2, t._1))
      .sortByKey(false)
      .map(t => (t._2, t._1))

    println("Number of words: "  + wordCount.count())

    def uuid = java.util.UUID.randomUUID.toString

    val jsonMap = wordCount.map(
      tuple => {
        val content = JsonArray.create()
        content.add(JsonObject.create().put("word", tuple._1).put("count", tuple._2))
        JsonArrayDocument.create(uuid, content)
      }).saveToCouchbase()
  }



}
