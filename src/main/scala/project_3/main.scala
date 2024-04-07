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

  // case class VertexInfo(vertexId: Long, result: Int, degreeOfInterest: Int)

  // def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
  //   var remaining_vertices = g_in.numVertices
  //   // updated_g is the graph we will shrink
  //   var updated_g = g_in
  //   // returned_g is a copy of g_in, and we modify whether each vertex has a 1 (in the MIS)
  //   // OR a 0 (not in the MIS)
  //   var returned_g = g_in
  //   returned_g = returned_g.mapVertices {(id, attr) => -1 }
  //   val rand = new scala.util.Random
  //   var degrees = g_in.degrees
  //   while (remaining_vertices >= 1) {
  //     val newGraph = updated_g.mapVertices { case (vertexId, value) => {
  //       val degreeOfInterest: Int = degrees.filter { case (vertexId2, _) => vertexId2 == vertexId }.first()._2
  //       // get random integer 0 up to 2 * degreeOfInterest (exclusive)
  //       // val random_int = rand.nextInt(2*degreeOfInterest)
  //       // see if we get 0, so that the probability is set appropriately
  //       // val result = if (random_int == 0) 1 else 0
  //       val result = rand.nextInt(2)
  //       VertexInfo(vertexId, result, degreeOfInterest)
  //     }}
  //     // here, we write the merge function such that we get the neighbor with the highest degree and with a 1
  //     // or, if no neighbor has a 1, we get a neighbor with a 0. we do this so we can compare in the next step
  //     val highestCompetingNeighbors: VertexRDD[VertexInfo] = newGraph.aggregateMessages[VertexInfo](
  //       triplet => { // Map Function
  //         triplet.sendToDst(triplet.srcAttr)
  //         triplet.sendToSrc(triplet.dstAttr)
  //       },
  //       (vertexInfo1, vertexInfo2) => {
  //         if(vertexInfo1.degreeOfInterest >= vertexInfo2.degreeOfInterest && vertexInfo1.result == 1) {
  //           vertexInfo1
  //         }
  //         else if (vertexInfo2.result == 1) {
  //           vertexInfo2
  //         }
  //         else {
  //           vertexInfo1
  //         }
  //       }
  //     )
  //     // Join the original graph's vertices with the highestCompetingNeighbors RDD based on vertex ID
  //     val joinedVertices = newGraph.vertices.join(highestCompetingNeighbors)

  //     // Update the original graph's vertices based on the comparison
  //     // "Yes" means we want to add vertex to MIS, "No" means we do not.
  //     val decisions = joinedVertices.map { case (vertexId, (originalInfo, competingInfo)) =>
  //       // Compare originalInfo and competingInfo and decide if vertex joins MIS
  //       if (((originalInfo.degreeOfInterest >= competingInfo.degreeOfInterest) && originalInfo.result == 1)||(originalInfo.result == 1 && (competingInfo.result == -1))) {
  //         (vertexId, "Yes")
  //       } else {
  //         (vertexId, "No")
  //       }
  //     }
  //     // Filter decisions RDD to select vertices with a decision of "Yes"
  //     val mappedVertices = returned_g.vertices.join(decisions).mapValues { case (value, decision) => if (decision == "Yes") 1 else -1 }
  //     // we update returned_g with what we include in the MIS
  //     returned_g = returned_g.mapVertices { case (vertexId, _) =>
  //       mappedVertices.lookup(vertexId).headOption.getOrElse(0)
  //     }
  //     // now we go through our edges, delete everyone who has a "Yes" in the triplet. so we delete all vertices with a "Yes" as well as their neighbors
  //     updated_g = updated_g.subgraph(epred = triplet => {
  //       val srcDecision = decisions.lookup(triplet.srcId).headOption.getOrElse("No")
  //       val dstDecision = decisions.lookup(triplet.dstId).headOption.getOrElse("No")
  //       srcDecision != "Yes" && dstDecision != "Yes"
  //     })
  //     remaining_vertices = updated_g.numVertices
  //     returned_g.cache()
  //     updated_g.cache()
  //   }
  //   return returned_g
  // }

  case class VertexProperties(id: Long, degree: Int, value: Double, active: String)

  // Define a function to see if any vertex is active
  def anyActive(g: Graph[VertexProperties, Int]): Boolean = {
    g.vertices.filter(
      { case (_, prop) => prop.active == "active"}
      ).count() > 0
  }

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    // Create random generator
    // Keep track of what vertex IDs are in the MIS
    var mis_vertices = Set[Long]()
    // Initialize graph to be modified (add properties)
    val degrees = g_in.degrees
    var degree_graph = g_in.outerJoinVertices(degrees)({ case (_, _, prop) => prop.getOrElse(0)})
    var g_mod = degree_graph.mapVertices((id, degree) => VertexProperties(id, degree, -0.1, "active"))
    println("Initial Size of graph: " + g_mod.numVertices)
    // Each vertex now stored as (vertexId, (zero_or_one, active))
    // Loop while active vertices exist
    var iterations = 0
    while(anyActive(g_mod)){
      // Step 1 - set values
      g_mod = g_mod.mapVertices((_, prop: VertexProperties) => {
        // Don't touch inactive vertices
        if (prop.active != "active") {
          prop
        } else {
          // YOU MUST GENERATE A RANDOM VARIABLE WITHIN THIS BLOCK
          val rand = new scala.util.Random
          // val value = if (rand.nextDouble() < 1.0 / (2 * prop.degree)) 1 else 0
          VertexProperties(prop.id, prop.degree, rand.nextDouble(), prop.active)
        }
      })
      // Step 2 - send message to neighbors and get highest competing neighbor
      // Compare the degree of the neighbors and the zero_or_one value
      val highest_neighbor: VertexRDD[VertexProperties] = g_mod.aggregateMessages[VertexProperties](
        triplet => { // Map Function
          triplet.sendToDst(triplet.srcAttr)
          triplet.sendToSrc(triplet.dstAttr)
        },
        (prop1, prop2) => {
          if (prop1.value > prop2.value) {
            prop1
          } else {
            prop2
          }
        }
      )
      // Join the original graph's vertices with the highest_neighbor RDD based on vertex ID
      val joinedVertices = g_mod.vertices.join(highest_neighbor).filter({
        case (_, (prop, competing)) => prop.active == "active"
      })
      // println("# of active vertices: " + joinedVertices.count())
      // Filter out vertices that lose to one of their neighbors (i.e. neighbors are in MIS)
      val message_comparison = joinedVertices.filter({
        case (id, (prop: VertexProperties, competing: VertexProperties)) => {
          // Check if the vertex is a local maxima (if true then add to MIS)
          prop.value > competing.value 
        }
      })
      // Step 3 - Extract all vertices that have survived the comparison (will be added to MIS)
      val vertexIds_mis = message_comparison.map({ case ((id, (prop, competing))) => prop.id}).collect().toSet
      println("\tNumber of vertices added to MIS: " + vertexIds_mis.size)
      // Add to the MIS
      mis_vertices = mis_vertices.union(vertexIds_mis)
      // Step 4 - check if each vertex or a neighbor of each vertex is in the MIS
      // Get the id of every vertex where the neighbor is in the MIS
      val neighbor_mis = g_mod.triplets.filter(e => (vertexIds_mis.contains(e.srcId) || vertexIds_mis.contains(e.dstId)))
      val vertexIds_mis_neighbors = neighbor_mis.map(e => e.srcId).collect().toSet.union(neighbor_mis.map(e => e.dstId).collect().toSet)
      // Join the two sets (neighbors + vertices in MIS)
      val vertexIds_mis_total = vertexIds_mis.toSet.union(vertexIds_mis_neighbors)
      // Step 5 - update the graph and deactivate necessary vertices
      g_mod = g_mod.mapVertices((id, prop) => {
        if (vertexIds_mis_total.contains(id)) {
          VertexProperties(prop.id, prop.degree, -0.1, "inactive")
        } else {
          prop
        }
      })
      // Cache the graph
      g_mod.cache()
      iterations += 1
    }
    print("Number of iterations: " + iterations)
    // Set vertices to 1 if in MIS, 0 otherwise
    val out_graph = g_in.mapVertices((id, _) => if (mis_vertices.contains(id)) 1 else -1)
    // Return the graph
    return out_graph
  }

  def hasNeighborWithAttributeOne(triplet: EdgeContext[Int, Int, Boolean], from: String): Boolean = {
    if (triplet.srcAttr == 1 && from == "to_dst") {
        return true
    } else if (triplet.srcAttr == -1 && from == "to_dst") {
        return false
    } else if (triplet.dstAttr == 1 && from == "to_src") {
        return true
    } else {
        return false
    }
  }

  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    val faulty_count = g_in.triplets.filter(e => (e.srcAttr == 1 && e.dstAttr == 1)).count
    val hasNeighborWithAttributeOneRDD = g_in.aggregateMessages[Boolean](
    triplet => { // Map Function
      triplet.sendToSrc(hasNeighborWithAttributeOne(triplet, "to_src"))
      triplet.sendToDst(hasNeighborWithAttributeOne(triplet, "to_dst"))
    },
    (bool1, bool2) => bool1 || bool2 // Reduce Function
    )
    val joinedVertices = hasNeighborWithAttributeOneRDD.join(g_in.vertices)

    // Filter the joined RDD to find vertices with attribute 0 and hasNeighborWithAttributeOne = false
    val filteredVertices = joinedVertices.filter {
      case (_, (hasNeighbor, vertexAttr)) => vertexAttr == -1 && !hasNeighbor
    }

    // Check if any vertex exists in g_in with the specified conditions
    val vertexExists: Boolean = filteredVertices.count() > 0
    if(faulty_count > 0 || vertexExists) {
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
      println("\n==================================")
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
