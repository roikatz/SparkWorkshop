import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

// creating a case class
case class Actor(id: String, name: String, moviesCount: Int)

/**
 * This demo shows the creation of SchemaRDDs from RDD[String] and JSON files
 * simple SQL execution and Scala UDFs
 */
object SparkSQL {

  def main (args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark SQL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc) // as we have spark context so do we have a context for SQL

    import sqlContext.implicits._ // in order to enable schema creation by reflection

    // create an RDD for the actor file
    val actorsFile = sc.textFile("/Users/roikatz/Documents/Data/actors")

    // 100 most popular
    val actors = actorsFile.map(_.split(",")).map(a => Actor(id = a(0).drop(1), name = a(1), moviesCount = a(2).dropRight(1).toInt)).toDF()

    actors.registerTempTable("actors")

    // 10 MOST PLAYED ACTORS IN MOVIES
    val topTierActors = sqlContext.sql("SELECT name,moviesCount FROM actors ORDER BY moviesCount DESC LIMIT 10")

    topTierActors.foreach(println)
    //---------------------------------------------------------------------------------
    sqlContext.cacheTable("actors")

    // the movies RDD will have schema based on the underlying JSON structure
    // so we can simply register it as a table
    //val marvelActors = sqlContext.jsonFile("file:/Users/roadan/Work/Sessions/Spark/Demos/Data/marvel_actors.json") 
    val marvelActors = sqlContext.jsonFile("/Users/roikatz/Documents/Data/marvel_actors.json")
    marvelActors.registerTempTable("marvelActors")

    // let's create a UDF
    def stringify (a: ArrayBuffer[String]) = a.mkString(",")
    sqlContext.udf.register("stringify", stringify _) // spark registerUDF
    val moviesWithFlatCast = sqlContext.sql("SELECT name, stringify(titles) FROM marvelActors")

    moviesWithFlatCast.foreach(println)
    //val hiveContext = new HiveContext(sc)

  }

}
