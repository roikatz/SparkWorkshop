import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object BondGraphX {

  def getActorName(actor_actor_data: Array[(Int, (String, String))], id: Int): String = {
    actor_actor_data(id)._2._1
  }

  def mapping(triplet: EdgeTriplet[(String, String), Int]): Iterator[(VertexId, (Int, String))] = {
    val character_name = triplet.srcAttr._1
    val src_type = triplet.srcAttr._2
    val dst_type = triplet.dstAttr._2

    if (src_type == "character" && dst_type == "actor") {
      Iterator((triplet.dstId, (1, character_name)))
    } else if (src_type == "actor" && dst_type == "character") {
      Iterator((triplet.srcId, (1, triplet.dstAttr._1)))
    } else {
      Iterator.empty
    }
  }

  def mappingAggregate(triplet: EdgeContext[(String, String), Int, (Int, String)]) {
    val character_name = triplet.srcAttr._1
    val src_type = triplet.srcAttr._2
    val dst_type = triplet.dstAttr._2

    if (src_type == "character" && dst_type == "actor") {
      triplet.sendToDst((1, character_name))
    } else if (src_type == "actor" && dst_type == "character") {
      triplet.sendToDst(1, triplet.dstAttr._1)
    }
  }

  def reduceCharacters(namesCount1: (Int, String), namesCount2: (Int, String)): (Int, String) = {
    ((namesCount1._1 + namesCount2._1), namesCount1._2 + ", " + namesCount2._2)
  }

  def main(args: Array[String]) {
    // Setup Spark Context
    val conf = new SparkConf().setAppName("bond_film_graph").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Setup graph:
    // 1.  Read edges from file
    // 2.  Read vertice data from file and join with edges
    val edge_only_graph = GraphLoader.edgeListFile(sc, "/Users/roikatz/Documents/Spark/GraphX/connections.txt")
    val names = sc.textFile("/Users/roikatz/Documents/Spark/GraphX/actors.txt")
      .map { line =>
             val fields = line.split(",")
             (fields(0).toInt, (fields(1), fields(2)))
    }

    val actor_data = names.collect()
    val full_graph = edge_only_graph.mapVertices((id, _) => actor_data(id.toInt)._2)

    // Aggregate actor-character relations - find out how many characters each actor played
    //    val actor_vertices = full_graph.mapReduceTriplets[(Int, String)](mapping, reduceCharacters) // old method

    val actor_vertices = full_graph.aggregateMessages[(Int, String)](mappingAggregate, reduceCharacters)

    // Extract actors who played more than one role
    val more_than_one_role = actor_vertices.filter { case (_, (count, _)) => count > 1}

    // Display results
    more_than_one_role.mapValues((id, data) =>
      data match {
        case (_, names) => getActorName(actor_data, id.toInt) + " AS " + names
      }
    )
      .collect()
      .foreach({ case (_, description) => println(description)})
  }
}
