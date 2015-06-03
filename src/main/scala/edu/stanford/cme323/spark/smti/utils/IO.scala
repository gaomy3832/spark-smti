package edu.stanford.cme323.spark.smti.utils

import java.io.File

import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import edu.stanford.cme323.spark.smti._


object IO {

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
}
