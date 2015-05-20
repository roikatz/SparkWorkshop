import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._


object SparkJoin {
  def main(args: Array[String])  {
    val conf = new SparkConf().setAppName("SparkJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)

//    //Part1
//    val tests1seq  = Seq(("Test1", 99), ("Test2", 86), ("Test3",76), ("Test4", 44))
//    val tests2seq  = Seq(("Test1", 66), ("Test2", 32), ("Test3",59), ("Test5", 33))
//
//    val tests1 = sc.parallelize(tests1seq)
//    val tests2 = sc.parallelize(tests2seq)
//
//    val joined = tests1.rightOuterJoin(tests2)
//
//    joined.foreach(println)

    //Part2

    //WordCount for shakespeare
    val spWC = sc.textFile("/Users/roikatz/Documents/Spark/Data/books/The Complete Works of William Shakespeare by William Shakespeare.txt")
                  .flatMap(line=>line.split(" ")).filter(s=> !s.isEmpty && s != null)
                  .map(w => (w,1)).reduceByKey(_+_).sortByKey(false)



    // WordCount for kamasutra
    val ksWC = sc.textFile("/Users/roikatz/Documents/Spark/Data/books/2.The Kama Sutra of Vatsyayana by Vatsyayana.txt")
      .flatMap(line=>line.split(" ")).filter(s=> !s.isEmpty && s != null)
      .map(w => (w,1)).reduceByKey(_+_).sortByKey(false)

    spWC.take(15).foreach(println)
    ksWC.take(15).foreach(println)

    val booksJoined = spWC.join(ksWC)
    booksJoined.take(15).foreach(println) // the first 15 words that exists on each book with thier wordcount

    val wordThe = booksJoined.filter(word => word._1 == "the")
    wordThe.foreach(println) // how many times the word the is on both books

  }
}
