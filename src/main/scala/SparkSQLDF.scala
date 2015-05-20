import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructField, StructType,StringType}
case class Person(name: String, age: Int)


object SparkSQLDF {
  def main ( args: Array[String]) = {
    val conf = new SparkConf()
      .setMaster("local[*]").setAppName("SparkDataFrames Tests")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val persons = sc.textFile("/Users/roikatz/Documents/Spark/Data/persons.txt")


    //Using reflection
    import sqlContext.implicits._

    val peopleRef = persons.map(_.split(" ")).map(t=> Person(t(0), t(1).trim.toInt)).toDF()
    peopleRef.printSchema

//
    peopleRef.registerTempTable("peopleRef")
    val peopleRefQuery = sqlContext.sql("Select * from peopleRef where age < 30")
    peopleRefQuery.map(t => "Name: " + t(0) + "  Age: " + t(1)).collect().foreach(println)
//
//    println("****************************************************")
//
//    // schema from string
    val schemaString = "name age"
    val schema = StructType(schemaString.split(" ")
      .map(f => StructField(f, StringType, false)))
    val personsRDD = persons.map(_.split(" "))
            .map(p => Row(p(0), p(1).trim()))

    val peopleDataFrame = sqlContext.createDataFrame(personsRDD, schema)
    peopleDataFrame.printSchema

    peopleDataFrame.registerTempTable("people")

    val result = sqlContext.sql("Select * from people where age > 22")
//
    result.map(t => "Name: " + t(0) + "  Age: " + t(1)).collect().foreach(println)
//


  }
}
