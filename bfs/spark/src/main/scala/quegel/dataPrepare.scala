import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
/**
 * Created by canoe on 5/7/15.
 */

object dataPrepare {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf)
     sc.textFile(args(0))
    .filter(line => !line.startsWith("#"))
    .map(line => line.split("\t"))
    .filter(_.length == 2)
    .map(x => (x(0) + "\t#\t", x(1)))
    .reduceByKey((a , b) => a + "\t" + b)
    .map(line => line.toString().replaceAll("[(),]", ""))
    .saveAsTextFile(args(1))
  }
}
