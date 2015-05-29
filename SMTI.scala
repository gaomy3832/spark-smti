import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd._

import scala.io.Source
import java.io.File

object SMTI {

  def main(args: Array[String]) {

    if (args.length != 1) {
      println("Usage: /path/to/spark/bin/spark-submit " + "--class SMTI "
        + "target/scala-*/smti-assembly-*.jar " + "prefListDir")
      sys.exit(1)
    }

    // Set up Spark environment

    val conf = new SparkConf().setAppName("SMTI")
    val sc = new SparkContext(conf)

    // Load data

    val prefListDir = args(0)

    val config = loadConfig(prefListDir)
    val men = loadRawPrefList(sc, prefListDir, "men.list")
    val women = loadRawPrefList(sc, prefListDir, "women.list")

    println("Men's list:")
    printRawPrefList(men, 10)
    println("Women's list:")
    printRawPrefList(women, 10)


    // Clean up
    sc.stop()
  }

  def loadConfig(dir: String): Array[Long] = {
    val lines = Source.fromFile(new File(dir, "config.txt")).getLines().toArray
    assert(lines.length == 1)
    lines(0).split(" ").map( x => x.toLong )
  }

  /* Raw preference list for one person.
   * From the most preferred to the least.
   * Ties are represented using negative indices, i.e., negative index has the
   * same preference as the one before it.
   */
  type RawPrefList = Array[Long]

  def loadRawPrefList(sc: SparkContext, dir: String, file: String): RDD[(Long, RawPrefList)] = {
    sc.textFile(new File(dir, file).toString).map { line =>
      val spLine = line.split(":")
      assert(spLine.length == 2)
      val index = spLine(0).toLong
      val prefList: RawPrefList = spLine(1).trim().split(" ").map( x => x.toLong )
      (index, prefList)
    }
  }

  def printRawPrefList(kvPrefList: RDD[(Long, RawPrefList)], num: Int) = {
    kvPrefList.take(num)
      .map( item => item._1.toString + ":" + item._2.mkString(" ") )
      .foreach( x => println(x) )
  }
}
