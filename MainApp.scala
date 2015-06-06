import java.io.File

import scala.io.Source

import org.apache.commons.io.FileUtils

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import edu.stanford.cme323.spark.smti._
import edu.stanford.cme323.spark.smti.utils.{IO, DataGen}


object MainApp {

  val usage = "Usage: <path/to/spark>/bin/spark-submit " +
    "--class edu.stanford.cme323.spark.smti.MainApp " +
    "target/scala-*/smti-assembly-*.jar\n" +
    "[--num-parts <P>=2] " +
    "[--in <pref list dir>=\"\"] " +
    "[--out <output dir>=\"\"] " +
    "[--size <n>=100] [--pt <pt>=0.2] [--pi <pi>=0.8] [--seed <seed>]"

  type OptMap = Map[Symbol, String]

  def parseOptions(map: OptMap, args: List[String]): OptMap = {
    // http://stackoverflow.com/questions/2315912/scala-best-way-to-parse-command-line-parameters-cli
    args match {
      case "--num-parts" :: value :: tail =>
        parseOptions(map + ('P -> value), tail)
      case "--in" :: value :: tail =>
        parseOptions(map + ('IN -> value), tail)
      case "--out" :: value :: tail =>
        parseOptions(map + ('OUT -> value), tail)
      case "--size" :: value :: tail =>
        parseOptions(map + ('N -> value), tail)
      case "--pt" :: value :: tail =>
        parseOptions(map + ('PT -> value), tail)
      case "--pi" :: value :: tail =>
        parseOptions(map + ('PI -> value), tail)
      case "--seed" :: value :: tail =>
        parseOptions(map + ('SEED -> value), tail)
      case option :: tail =>
        println("Unknown option: " + option)
        println(usage)
        sys.exit(1)
      case Nil =>
        map
    }
  }

  def main(args: Array[String]) {

    // Set logging
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // Set up Spark environment
    val conf = new SparkConf().setAppName("SMTI")
    val sc = new SparkContext(conf)

    // Parse options
    val optMap = parseOptions(Map(), args.toList)

    // Generate or load data
    var menPrefLists: RDD[(Index, PrefList)] = null
    var womenPrefLists: RDD[(Index, PrefList)] = null

    val numPartitions = optMap.getOrElse('P, "2").toInt
    val prefListDir = optMap.getOrElse('IN, "")
    val outDir = optMap.getOrElse('OUT, "")
    if (prefListDir.isEmpty) {
      val n = optMap.getOrElse('N, "100").toLong
      val pt = optMap.getOrElse('PT, "0.2").toDouble
      val pi = optMap.getOrElse('PI, "0.8").toDouble
      val seed = optMap.getOrElse('SEED, System.currentTimeMillis().toString).toLong
      val genPrefListTuple = DataGen.generate(sc, n, pi, pt, seed)
      menPrefLists = genPrefListTuple._1
      womenPrefLists = genPrefListTuple._2
    } else {
      menPrefLists = IO.loadModifiedRGSIPrefLists(sc, prefListDir, "men.list")
      womenPrefLists = IO.loadModifiedRGSIPrefLists(sc, prefListDir, "women.list")
    }

    // SMTI solver
    val solver = new SMTIGSKiraly(menPrefLists, womenPrefLists, numPartitions)

    val tStart = System.nanoTime()
    solver.run()
    val tEnd = System.nanoTime()
    println("Elapsed time: " + (tEnd - tStart) / 1e6 + " ms")

    // Result
    println("Verify results ... " + {if (solver.verify()) "passed!" else "failed!"})
    val marriage = solver.marriage()
    println("Result marriage size = " + marriage.count())

    // Write out
    if (!outDir.isEmpty) {
      solver.marriage().saveAsTextFile("marriage")
    }

    // Clean up
    sc.stop()
  }

}
