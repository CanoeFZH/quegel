package quegel

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.Analytics
import scala.reflect.ClassTag
import scala.Array.canBuildFrom

object bfs {

  val SOURCE_LIST = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
  val DEST_LIST = Array(9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

  val SEPARATOR = "[\t ]"

  def loadUndirectedGraph[VD: ClassTag, ED: ClassTag](sc: SparkContext, path: String, defaultEdgeAttr: ED, defaultVetexAttr: VD): Graph[VD, ED] =
    {
      val textRDD = sc.textFile(path);
      val edge = textRDD.flatMap(
        line => {
          val numbers = line.split(SEPARATOR);
          val srcId: VertexId = numbers(0).trim.toLong;
          numbers.slice(2, numbers.size).map(num => Edge(srcId, num.trim.toLong, defaultEdgeAttr)).filter(p => p.srcId != p.dstId)
        })
      Graph.fromEdges[VD, ED](edge, defaultVetexAttr);
    }

  def loadDirectedGraph[VD: ClassTag, ED: ClassTag](sc: SparkContext, path: String, defaultEdgeAttr: ED, defaultVetexAttr: VD): Graph[VD, ED] =
    {
      val textRDD = sc.textFile(path);
      val edge = textRDD.flatMap(
        line => {
          val numbers = line.split(SEPARATOR);
          val srcId: VertexId = numbers(0).trim.toLong;
          val inNeighborsNum = numbers(1).trim.toInt;
          numbers.slice(3 + inNeighborsNum, numbers.size).map(num => Edge(srcId, num.trim.toLong, defaultEdgeAttr)).filter(p => p.srcId != p.dstId)
        })
      Graph.fromEdges[VD, ED](edge, defaultVetexAttr);
    }

  def SingleSourceBFS(sc: SparkContext, inputPath: String, outputPath: String): (Double, Double, Double) = {
    var startTime = System.currentTimeMillis
    val graph = loadUndirectedGraph(sc, inputPath, 1, 1).partitionBy(PartitionStrategy.RandomVertexCut)
    val loadtime = System.currentTimeMillis - startTime

    var computetime = 0.0
    var dumpTime = 0.0

    for (i <- 0 until SOURCE_LIST.length) {
      val SOURCE_VERTEX = SOURCE_LIST(i)
      val DEST_VERTEX = DEST_LIST(i)

      startTime = System.currentTimeMillis

      var bfsGraph = graph.mapVertices((id, _) => if (id == SOURCE_VERTEX) 0 else Int.MaxValue)

      var newVisited: Long = 1
      var found = false
      var superstep = 0

      //bfsGraph.vertices.collect().foreach(f => println("######## " + f._1 + " " + f._2))

      while (newVisited > 0 && !found) {

        superstep += 1

        bfsGraph = bfsGraph.pregel(Int.MaxValue, 1)(
          (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
          triplet => { // Send Message
            if (triplet.srcAttr != Int.MaxValue && triplet.dstAttr == Int.MaxValue) {
              Iterator((triplet.dstId, superstep))
            } else {
              Iterator.empty
            }
          },
          (a, b) => a // Merge Message
          )

        newVisited = bfsGraph.vertices.filter(f => f._2 == superstep).count
        found = bfsGraph.vertices.filter { case (id, dis) => id == DEST_VERTEX && dis != Int.MaxValue }.count != 0
        //bfsGraph.vertices.collect().foreach(f => println("######## " + f._1 + " " + f._2))
      }

      computetime += System.currentTimeMillis - startTime

      startTime = System.currentTimeMillis
      val curOutputPath = outputPath + "_" + i

      val result = bfsGraph.vertices.filter { case (id, dis) => id == DEST_VERTEX }
      result.saveAsTextFile(curOutputPath)
      dumpTime += System.currentTimeMillis - startTime

    }
    (loadtime, computetime, dumpTime)
  }

  def BiBFS(sc: SparkContext, inputPath: String, outputPath: String): (Double, Double, Double) = {
    var startTime = System.currentTimeMillis
    val graph = loadDirectedGraph(sc, inputPath, (1, 1), 1).partitionBy(PartitionStrategy.RandomVertexCut)
    val loadtime = System.currentTimeMillis - startTime

    //System.out.println("### edges: " + graph.edges.count + " vertices: " + graph.vertices.count)

    var computetime = 0.0
    var dumpTime = 0.0

    for (i <- 0 until SOURCE_LIST.length) {
      val SOURCE_VERTEX = SOURCE_LIST(i)
      val DEST_VERTEX = DEST_LIST(i)

      startTime = System.currentTimeMillis

      var bfsGraph = graph.mapVertices((id, _) =>
        id match {
          case SOURCE_VERTEX => (0, Int.MaxValue)
          case DEST_VERTEX => (Int.MaxValue, 0)
          case _ => (Int.MaxValue, Int.MaxValue)
        })

      var forwardVisited: Long = 1
      var backwardVisited: Long = 1

      var found = false
      var superstep = 0

      //bfsGraph.vertices.collect().foreach(f => println("######## " + f._1 + " " + f._2._1 + " " + f._2._2))

      while (forwardVisited > 0 && forwardVisited > 0 && !found) {

        superstep += 1
        //forward
        bfsGraph = bfsGraph.pregel(Int.MaxValue, 1)(
          (id, dist, newDist) => (math.min(dist._1, newDist), dist._2), // Vertex Program
          triplet => { // Send Message
            if (triplet.srcAttr._1 != Int.MaxValue && triplet.dstAttr._1 == Int.MaxValue) {
              Iterator((triplet.dstId, superstep))
            } else {
              Iterator.empty
            }
          },
          (a, b) => a // Merge Message
          )

        //bfsGraph.vertices.collect().foreach(f => println("######## " + f._1 + " " + f._2._1 + " " + f._2._2))

        //backward
        bfsGraph = bfsGraph.pregel(Int.MaxValue, 1)(
          (id, dist, newDist) => (dist._1, math.min(dist._2, newDist)), // Vertex Program
          triplet => { // Send Message
            if (triplet.dstAttr._2 != Int.MaxValue && triplet.srcAttr._2 == Int.MaxValue) {
              Iterator((triplet.srcId, superstep))
            } else {
              Iterator.empty
            }
          },
          (a, b) => a // Merge Message
          )

        forwardVisited = bfsGraph.vertices.filter(f => f._2._1 == superstep).count
        backwardVisited = bfsGraph.vertices.filter(f => f._2._2 == superstep).count

        found = bfsGraph.vertices.filter { case (_, dis) => dis._1 != Int.MaxValue && dis._2 != Int.MaxValue }.count != 0
        //bfsGraph.vertices.collect().foreach(f => println("######## " + f._1 + " " + f._2._1 + " " + f._2._2))
      }

      computetime += System.currentTimeMillis - startTime

      startTime = System.currentTimeMillis
      val curOutputPath = outputPath + "_" + i

      var result = -1

      if (found) {
        result = bfsGraph.vertices.flatMap(attr => {
          val fdist = attr._2._1
          val bdist = attr._2._2

          if (fdist == Int.MaxValue || bdist == Int.MaxValue)
            Iterator.empty
          else
            Iterator(fdist + bdist)
        }).reduce((a, b) => math.min(a, b))

      }

      //System.out.println("#### " + DEST_VERTEX + " " + result)
      sc.parallelize(Seq((DEST_VERTEX, result)), 1).saveAsTextFile(curOutputPath)

      dumpTime += System.currentTimeMillis - startTime

    }
    (loadtime, computetime, dumpTime)
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "bfs")
    val inputPath = args(1)
    val outputPath = args(2)
    val cmd = args(3)

    val times =
      cmd match {
        case "bfs" => {
          SingleSourceBFS(sc, inputPath, outputPath)
        }
        case "biBfs" => {
          BiBFS(sc, inputPath, outputPath)
        }
        case _ => {
          System.out.println("Wrong parameters!")
          (0.0, 0.0, 0.0)
        }
      }

    System.out.println("Loading Graph in " + times._1 + " ms.")
    System.out.println("Finished Running engine in " + times._2 + " ms.")
    System.out.println("Dumping Graph in " + times._3 + " ms.")
  }
}