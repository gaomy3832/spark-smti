import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

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

    println(config(0))



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
}
