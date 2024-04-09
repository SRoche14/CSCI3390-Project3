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

  case class VertexProperties(id: Long, degree: Int, value: Double, active: String, neighbor_in_mis: Boolean, in_mis: Boolean)

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
    var g_mod = degree_graph.mapVertices((id, degree) => VertexProperties(id, degree, -0.1, "active", false, false))
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
          VertexProperties(prop.id, prop.degree, rand.nextDouble(), prop.active, prop.neighbor_in_mis, prop.in_mis)
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
      val joinedVertices = g_mod.vertices.join(highest_neighbor)
      // If a vertex is a local maxima set it to be in the MIS
      val message_comparison = joinedVertices.map({
        case (id, (prop: VertexProperties, competing: VertexProperties)) => {
          // Check if the vertex is a local maxima (if true then add to MIS)
          if ((prop.active == "active") && (prop.value > competing.value)) {
            // Add to MIS (set in_mis = true)
            (id, VertexProperties(prop.id, prop.degree, prop.value, "inactive", false, true))
          } else {
            (id, prop)
          }
        }
      })
      // Set message_comparison as data for every vertex
      val comparison_graph = g_mod.outerJoinVertices(message_comparison)({
        case (_, prop, new_prop) => new_prop.getOrElse(prop)
      })
      // Step 3 - update the graph with the new values
      val neighbor_in_mis: VertexRDD[VertexProperties] = comparison_graph.aggregateMessages[VertexProperties](
        triplet => {
          triplet.sendToDst(triplet.srcAttr)
          triplet.sendToSrc(triplet.dstAttr)
        },
        (prop1, prop2) => {
          // Only update flags of vertices that haven't been altered
          if ((prop1.active =="active") && (prop1.in_mis == false) && (prop1.neighbor_in_mis == false)) {
            if (prop2.in_mis == true) {
              VertexProperties(prop1.id, prop1.degree, prop1.value, "inactive", true, false)
            } else {
              prop1
            }
          } else {
            prop1
          }
        }
      )
      g_mod = g_mod.outerJoinVertices(neighbor_in_mis)({
        case (_, prop, new_prop) => new_prop.getOrElse(prop)
      })
      // Step 5 - update the graph and deactivate necessary vertices
      // g_mod = neighbor_graph.mapVertices((id, prop) => {
      //   if ((prop.in_mis == true) || (prop.neighbor_in_mis == true)) {
      //     VertexProperties(prop.id, prop.degree, prop.value, "inactive", prop.in_mis, prop.neighbor_in_mis)
      //   } else {
      //     prop
      //   }
      // })
      // Cache the graph
      g_mod = g_mod.cache()
      println("\tNumber of vertices remaining: " + g_mod.vertices.filter({ case (_, prop) => prop.active == "active"}).count())
      iterations += 1
    }
    print("Number of iterations: " + iterations)
    // Set vertices to 1 if in MIS, 0 otherwise
    val out_graph = g_mod.mapVertices((id, prop) => if (prop.in_mis) 1 else -1)
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
