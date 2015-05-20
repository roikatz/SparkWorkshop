import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._


object Operations {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Operations").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val tests = Seq(("Test1", 77),("Test1", 76), ("Test2", 88), ("Test3", 65), ("Test2", 76), ("Test2", 87),
      ("Test1", 67), ("Test1", 98), ("Test1", 78))
    val testsRdd = sc.parallelize(tests)


    val tests2 = Seq(("Test1", 33), ("Test2", 44), ("Test3", 55), ("Test3", 66))
    val testsRdd2 = sc.parallelize(tests2)

//    val cogroup = testsRdd.cogroup(testsRdd2) // ("Test1" , ((groupA), GroupB)))
//    println("*********Start CoGroup***********")
//    cogroup.foreach(println)
//    println("*********End CoGroup***********")
////
////
////    //groupByKey
//    val testsGroupByKey = testsRdd.groupByKey() // SIMILAR TO WHAT THE REDUCER IN HADOOP IS GETTING
//    println("*********Start GroupByKey***********")
//    testsGroupByKey.foreach(println)
//    println("*********End GroupByKey***********")
////
////    //reduce
////    println("*********Start Reduce***********")
    val testsReduceValue = testsRdd.reduce((a, b) => (("sum"), (a._2 + b._2)))
////    println("*********End Reduce***********")
////
////
////    //reduceByKey
//    val testsReduce = testsRdd.reduceByKey(_ + _).collect()
////    println("*********Start ReduceByKey***********")
//    testsReduce.foreach(println)
////    println("*********End ReduceByKey***********")
////
////
////    //foldByKey
////
//    val testsFold = testsRdd.foldByKey(0)((carryOver, e) => carryOver + e )
////    println("*********Start FoldByKey***********")
////    //val testsFold = testsRdd.foldByKey(0)(_ + _)
//    testsFold.foreach(println)
////    println("*********End FoldByKey***********")

//    println("*********Start AggregateByKey***********")
//    val testAggregate = testsRdd.aggregateByKey(0)((k,v) => k+v, (k,v)=>k+v)
//    testAggregate.foreach(println)

  }

}
