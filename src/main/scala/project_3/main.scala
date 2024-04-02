package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
// main function
object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  case class VertexInfo(vertexId: Long, result: Int, degreeOfInterest: Int)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var remaining_vertices = g_in.numVertices
    // updated_g is the graph we will shrink
    var updated_g = g_in
    // returned_g is a copy of g_in, and we modify whether each vertex has a 1 (in the MIS)
    // OR a 0 (not in the MIS)
    var returned_g = g_in
    returned_g = returned_g.mapVertices {(id, attr) => 0 }
    val rand = new scala.util.Random
    var degrees = g_in.degrees
    while (remaining_vertices >= 1) {
      val newGraph = updated_g.mapVertices { case (vertexId, value) => {
        val degreeOfInterest: Int = degrees.filter { case (vertexId2, _) => vertexId2 == vertexId }.first()._2
        // get random integer 0 up to 2 * degreeOfInterest (exclusive)
        val random_int = rand.nextInt(2*degreeOfInterest)
        // see if we get 0, so that the probability is set appropriately
        val result = if (random_int == 0) 1 else 0
        VertexInfo(vertexId, result, degreeOfInterest)
      }}
      // here, we write the merge function such that we get the neighbor with the highest degree and with a 1
      // or, if no neighbor has a 1, we get a neighbor with a 0. we do this so we can compare in the next step
      val highestCompetingNeighbors: VertexRDD[VertexInfo] = newGraph.aggregateMessages[VertexInfo](
        triplet => { // Map Function
          triplet.sendToDst(triplet.srcAttr)
          triplet.sendToSrc(triplet.dstAttr)
        },
        (vertexInfo1, vertexInfo2) => {
          if(vertexInfo1.degreeOfInterest >= vertexInfo2.degreeOfInterest && vertexInfo1.result == 1) {
            vertexInfo1
          }
          else if (vertexInfo2.result == 1) {
            vertexInfo2
          }
          else {
            vertexInfo1
          }
        }
      )
      // Join the original graph's vertices with the highestCompetingNeighbors RDD based on vertex ID
      val joinedVertices = newGraph.vertices.join(highestCompetingNeighbors)

      // Update the original graph's vertices based on the comparison
      // "Yes" means we want to add vertex to MIS, "No" means we do not.
      val decisions = joinedVertices.map { case (vertexId, (originalInfo, competingInfo)) =>
        // Compare originalInfo and competingInfo and decide if vertex joins MIS
        if ((originalInfo.degreeOfInterest >= competingInfo.degreeOfInterest && originalInfo.result == 1)||(originalInfo.result == 1 && competingInfo.result == 0)) {
          (vertexId, "Yes")
        } else {
          (vertexId, "No")
        }
      }
      // Filter decisions RDD to select vertices with a decision of "Yes"
      val mappedVertices = returned_g.vertices.join(decisions).mapValues { case (value, decision) => if (decision == "Yes") 1 else 0 }
      // we update returned_g with what we include in the MIS
      returned_g = returned_g.mapVertices { case (vertexId, _) =>
        mappedVertices.lookup(vertexId).headOption.getOrElse(0)
      }
      // now we go through our edges, delete everyone who has a "Yes" in the triplet. so we delete all vertices with a "Yes" as well as their neighbors
      updated_g = updated_g.subgraph(epred = triplet => {
        val srcDecision = decisions.lookup(triplet.srcId)._2
        val dstDecision = decisions.lookup(triplet.dstId)._2
        srcDecision != "Yes" && dstDecision != "Yes"
      })
      remaining_vertices = updated_g.numVertices
    }
    return returned_g
  }


  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    val faulty_count = g_in.triplets.filter(e => (e.srcAttr == 1 && e.dstAttr == 1)).count
    if(faulty_count > 0) {
      return false
    }
    else {
      return true
    }
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")
      
      // also run MIS verify
      val ans = verifyMIS(g2)
      if(ans)
        println("Yes")
      else
        println("No")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
