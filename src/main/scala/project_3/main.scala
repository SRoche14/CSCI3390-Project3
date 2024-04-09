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

  case class VertexProperties(id: Long, degree: Int, value: Double, active: String, in_MIS: String)

  // Define a function to see if any vertex is active
  def anyActive(g: Graph[VertexProperties, Int]): Boolean = {
    g.vertices.filter(
      { case (_, prop) => prop.active == "active"}
      ).count() > 0
  }

  // Returns true if the RDD contains the value
  def inRDD(rdd: RDD[Long], value: Long): Boolean = {
    rdd.filter(e => e == value).count() > 0
  }

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    // Keep track of what vertex IDs are in the MIS
    var mis_vertices = Set[Long]()
    // Initialize graph to be modified (add properties)
    val degrees = g_in.degrees
    var degree_graph = g_in.outerJoinVertices(degrees)({ case (_, _, prop) => prop.getOrElse(0)})
    var g_mod = degree_graph.mapVertices((id, degree) => VertexProperties(id, degree, -0.1, "active", "No"))
    g_mod.cache()
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
          VertexProperties(prop.id, prop.degree, rand.nextDouble(), prop.active, prop.in_MIS)
        }
      })
      // Cache the graph
      // g_mod.cache()
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
      val vertexIds_mis = message_comparison.map({ case ((id, (prop, competing))) => prop.id}).distinct().collect().toSet
      // println("\tNumber of vertices added to MIS: " + vertexIds_mis.size)
      // Add to the MIS
      //mis_vertices = mis_vertices.union(vertexIds_mis)
      // mis_vertices = mis_vertices.map({case (id, _) => if (inRDD(vertexIds_mis, id)) (id, 1) else (id, -1)})
      // Step 4 - check if each vertex or a neighbor of each vertex is in the MIS
      // Get the id of every vertex where the neighbor is in the MIS
      // val neighbor_mis = g_mod.triplets.filter(e => (vertexIds_mis.contains(e.srcId) || vertexIds_mis.contains(e.dstId)))
      // val mis_neighbors = neighbor_mis.map(e => e.srcId).collect().toSet.union(neighbor_mis.map(e => e.dstId).collect().toSet)
      // select source and destination vertices from the neighbor_mis RDD
      // val mis_neighbors1 = neighbor_mis.map(e => e.srcId).collect().toSet
      // val mis_neighbors2 = neighbor_mis.map(e => e.dstId).collect().toSet
      // val mis_neighbors1 = g_mod.triplets.filter(e => (vertexIds_mis.contains(e.srcId))).map(e => e.dstId).collect().toSet
      // val mis_neighbors2 = g_mod.triplets.filter(e => (vertexIds_mis.contains(e.dstId))).map(e => e.srcId).collect().toSet
      val in_mis_graph = g_in.mapVertices((id, _) => vertexIds_mis.contains(id))
      val neighbor_in_mis: VertexRDD[Boolean] = in_mis_graph.aggregateMessages[Boolean](
        triplet => { // Map Function -> send a flag to the source and destination vertices on if the neighbor is in the MIS
          triplet.sendToDst(triplet.srcAttr)
          triplet.sendToSrc(triplet.dstAttr)
        },
        (flag1, flag2) => {
          flag1 || flag2
        }
      )
      val mis_neighbors = neighbor_in_mis.filter({ case (_, flag) => flag}).map({ case (id, _) => id}).distinct().collect().toSet
      // Join the two sets (neighbors + vertices in MIS)
      //val vertexIds_mis_total = vertexIds_mis.union(mis_neighbors)//.union(mis_neighbors2)
      // Step 5 - update the graph and deactivate necessary vertices
      g_mod = g_mod.mapVertices((id, prop) => {
        if (vertexIds_mis.contains(id)) {
          VertexProperties(prop.id, prop.degree, -0.1, "inactive", "Yes")
        }
        else if (mis_neighbors.contains(id)) {
          VertexProperties(prop.id, prop.degree, -0.1, "inactive", "No")
        } else {
          prop
        }
      })
      // Cache the graph
      // g_mod.cache()
      println("\tNumber of vertices remaining: " + g_mod.vertices.filter({ case (_, prop) => prop.active == "active"}).count())
      iterations += 1
    }
    print("Number of iterations: " + iterations)
    // val in_mis = mis_vertices.filter({ case (_, value) => value == 1}).map({ case (id, _) => id}).collect().toSet
    // Set vertices to 1 if in MIS, 0 otherwise
    val vertexRDD = g_mod.vertices

    // Filter vertices based on the condition prop.in_MIS == "Yes"
    val verticesInMIS: RDD[VertexId] = vertexRDD
      .filter { case (_, prop) => prop.in_MIS == "Yes" }
      .map { case (id, _) => id }

    // Collect the vertex IDs
    val vertexIDs: Array[VertexId] = verticesInMIS.collect()
    val out_graph = g_in.mapVertices((id, _) => if (vertexIDs.contains(id)) 1 else -1)
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
    /* hide INFO logs */
    spark.sparkContext.setLogLevel("ERROR")
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
      println("Starting LubyMIS")
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
