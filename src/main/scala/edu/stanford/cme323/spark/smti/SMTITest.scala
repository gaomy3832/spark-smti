package edu.stanford.cme323.spark.smti

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import java.io.File
import scala.io.Source

object SMTITest {

  def main(args: Array[String]) {

    if (args.length != 1) {
      println("Usage: /path/to/spark/bin/spark-submit "
        + "--class edu.stanford.cme323.spark.smti.SMTITest "
        + "target/scala-*/smti-assembly-*.jar " + "prefListDir")
      sys.exit(1)
    }

    val conf = new SparkConf().setAppName("SMTI")
    val sc = new SparkContext(conf)

    val prefListDir = args(0)

    val config = loadConfig(prefListDir)

    val smtiSolver = new SMTI(sc, config(0), prefListDir, "men.list", "women.list")
    smtiSolver.printStatus(10)

    // Clean up
    sc.stop()
  }

  def loadConfig(dir: String): Array[Long] = {
    val lines = Source.fromFile(new File(dir, "config.txt")).getLines().toArray
    assert(lines.length == 1)
    lines(0).split(" ").map( x => x.toLong )
  }

}
