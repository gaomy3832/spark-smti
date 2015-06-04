import java.io.File

import scala.io.Source

import org.apache.commons.io.FileUtils

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner

import edu.stanford.cme323.spark.smti._
import edu.stanford.cme323.spark.smti.utils.IO


object Main {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length < 1) {
      println("Usage: /path/to/spark/bin/spark-submit "
        + "--class edu.stanford.cme323.spark.smti.Main "
        + "target/scala-*/smti-assembly-*.jar "
        + "prefListDir " + "numPartitions")
      sys.exit(1)
    }

    val prefListDir = args(0)
    val numPartitions = { if (args.length >= 2) args(1).toInt else 2 }

    val conf = new SparkConf().setAppName("SMTI")
    val sc = new SparkContext(conf)

    val menPrefLists = IO.loadModifiedRGSIPrefLists(sc, prefListDir, "men.list")
      .partitionBy(new HashPartitioner(numPartitions))
    val womenPrefLists = IO.loadModifiedRGSIPrefLists(sc, prefListDir, "women.list")
      .partitionBy(new HashPartitioner(numPartitions))

    val smtiSolver = new SMTIGSKiraly(menPrefLists, womenPrefLists)

    val tStart = System.nanoTime()
    smtiSolver.run()
    val tEnd = System.nanoTime()
    println("Elapsed time: " + (tEnd - tStart) / 1e6 + " ms")

    println("Verify results ... " + {if (smtiSolver.verify()) "passed!" else "failed!"})
    val marriage = smtiSolver.marriage()
    println("Result marriage size = " + marriage.count())

    FileUtils.deleteDirectory(new File("marriage"))
    smtiSolver.marriage().saveAsTextFile("marriage")

    // Clean up
    sc.stop()
  }

}
