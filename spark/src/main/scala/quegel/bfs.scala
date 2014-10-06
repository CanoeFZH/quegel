package quegel

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.Analytics
import scala.Array.canBuildFrom
object bfs {
  def main(args: Array[String]) {
    val sc = new SparkContext("spark://localhost:7077", "bfs")
    val infile = sc.textFile(args(0))
    val lines = infile.map(line => line.split("\t"))
    val verticesRDD = lines.map(parts => (parts(0).toLong, null))
    val edgesRDD = lines.flatMap { parts =>
      val adj = parts(1).split(" ")
      val vid = parts(0).toLong
      for (i <- 1 until adj.length) yield Edge(vid, adj(i).toLong, null)
    }
    val graph = Graph(verticesRDD, edgesRDD)
    val pagerankGraph = graph.outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }.mapTriplets(e => 1.0 / e.srcAttr).mapVertices((id, attr) => 1.0).cache()
    val numIter = 10
    val resetProb = 0.15
    def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double = resetProb + (1.0 - resetProb) * msgSum
    def sendMessage(edge: EdgeTriplet[Double, Double]) = Iterator((edge.dstId, edge.srcAttr * edge.attr))
    def messageCombiner(a: Double, b: Double): Double = a + b
    val initialMessage = 0.0
    val res = Pregel(pagerankGraph, initialMessage, numIter, activeDirection = EdgeDirection.Out)(
      vertexProgram, sendMessage, messageCombiner)
    res.vertices.saveAsTextFile(args(1))
  }
}
