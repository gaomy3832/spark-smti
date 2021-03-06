package edu.stanford.cme323.spark.smti.utils

import java.io.File
import java.io.FileWriter

import scala.io.Source

import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import edu.stanford.cme323.spark.smti._


object IO extends Logging {

  /**
   * Modified Random generated SMTI instances (RGSI) format.
   * Gent, I. P. and Prosser, P. An Empirical Study of the Stable Marriage
   * Problem with Ties and Incomplete Lists. In ECAI 2002
   * Munera, D. et al. Solving Hard Stable Matching Problems via Local Search
   * and Cooperative Parallelization. In AAAI 2015
   *
   * Each line is as "index: raw preference list"
   * Raw preference list is the index of opposite sex people, ordered from the
   * most preferred to the least, and separated by space.
   * Ties are represented using negative indices, i.e., negative index has the
   * same preference rank as the one before it.
   */
  def loadModifiedRGSIPrefLists(sc: SparkContext, dir: String, file: String): RDD[(Index, PrefList)] = {
    logInfo(f"Load RGSI preference list from $dir/$file")

    sc.textFile(new File(dir, file).toString).map{ line =>
      val spLine = line.split(":")
      assert(spLine.length == 2)
      val index: Index = spLine(0).toLong
      val rawPrefList: Array[Index] = spLine(1).trim().split(" ").map( x => x.toLong )

      // Compose rank list
      var rankList = new Array[Rank](rawPrefList.length)
      var rank = 0
      var i = 0
      for ( i <- 0 until rawPrefList.length ) {
        // do not increase rank when index is negative
        if (rawPrefList(i) > 0) rank += 1
        rankList(i) = rank
      }

      val prefList = (rankList zip rawPrefList).map( tuple => Pref(tuple._1, tuple._2.abs) )

      (index, prefList)
    }
  }

  def storeModifiedRGSIPrefLists(sc: SparkContext, prefLists: RDD[(Index, PrefList)], dir: String, file: String) = {
    logInfo(f"Store RGSI preference list to $dir/$file")

    val rawLists: RDD[(Index, Array[Index])] = prefLists
      .mapValues{ prefList =>
        var rawPrefList: Array[Index] = new Array[Index](prefList.length)
        rawPrefList(0) = prefList(0).index
        for ( i <- 1 until prefList.length) {
          if (prefList(i).rank == prefList(i-1).rank) {
            rawPrefList(i) = -prefList(i).index
          } else {
            rawPrefList(i) = prefList(i).index
          }
        }
        rawPrefList
      }

    val content: Array[String] = rawLists
      .map{ case(index, rawPrefList) =>
        index.toString + ": " + rawPrefList.mkString(" ")
      }
      .collect()

    val writer = new FileWriter(new File(dir, file).toString)
    content.foreach(line => writer.write(line + "\n"))
    writer.close()
  }

}
