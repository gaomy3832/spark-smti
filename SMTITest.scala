import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner

import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io.File
import scala.io.Source

import edu.stanford.cme323.spark.smti._


object SMTITest {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length != 2) {
      println("Usage: /path/to/spark/bin/spark-submit "
        + "--class edu.stanford.cme323.spark.smti.SMTITest "
        + "target/scala-*/smti-assembly-*.jar "
        + "numPartitions " + "prefListDir")
      sys.exit(1)
    }

    val numPartitions = args(0).toInt
    val prefListDir = args(1)

    val conf = new SparkConf().setAppName("SMTI")
    val sc = new SparkContext(conf)

    val config = loadConfig(prefListDir)

    val menPrefLists = Input.loadModifiedRGSIPrefLists(sc, prefListDir, "men.list")
      .partitionBy(new HashPartitioner(numPartitions))
    val womenPrefLists = Input.loadModifiedRGSIPrefLists(sc, prefListDir, "women.list")
      .partitionBy(new HashPartitioner(numPartitions))

    val smtiSolver = new SMTIKiralyAlgo(menPrefLists, womenPrefLists)

    println(smtiSolver.verify())
    println(smtiSolver.sizeOfMarriage())

    val tStart = System.nanoTime()
    smtiSolver.run(10)
    val tEnd = System.nanoTime()

    println("Elapsed time: " + (tEnd - tStart) / 1e6 + " ms")

    println(smtiSolver.verify())
    println(smtiSolver.sizeOfMarriage())
    smtiSolver.results().filter( pair => pair._2 != InvIndex ).take(10).foreach(println)

    // Clean up
    sc.stop()
  }

  def loadConfig(dir: String): Array[Long] = {
    val lines = Source.fromFile(new File(dir, "config.txt")).getLines().toArray
    assert(lines.length == 1)
    lines(0).split(" ").map( x => x.toLong )
  }

}
