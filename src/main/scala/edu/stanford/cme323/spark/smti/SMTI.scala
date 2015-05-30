package edu.stanford.cme323.spark.smti

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

import java.io.File


class SMTI (sc: SparkContext, size: Long, dir: String, propFile: String, accpFile: String) {

  var proposers: RDD[(Index, PersonStatus)] = loadPrefList(dir, propFile)
  var acceptors: RDD[(Index, PersonStatus)] = loadPrefList(dir, accpFile)

  assert(proposers.count() == size)
  assert(acceptors.count() == size)



  /**
   * IO
   */
  private def loadPrefList(dir: String, file: String): RDD[(Index, PersonStatus)] = {
    sc.textFile(new File(dir, file).toString).map{ line =>
      // Index and raw preference list
      // Each raw pref list is ordered from the most preferred to the least.
      // Ties are represented using negative indices, i.e., negative index has the
      // same preference as the one before it.
      val spLine = line.split(":")
      assert(spLine.length == 2)
      val index: Index = spLine(0).toLong
      val rawPrefList: Array[Index] = spLine(1).trim().split(" ").map( x => x.toLong )

      // Compose rank list: if index is negative, has the same rank as the one before it.
      val rankList = new Array[Rank](rawPrefList.length)
      var rank = 0
      var i = 0
      for ( i <- 0 until rawPrefList.length ) {
        if (rawPrefList(i) > 0) rank += 1
        rankList(i) = rank
      }

      val prefList = (rankList zip rawPrefList).map( tuple => Pref(tuple._1, tuple._2.abs) )

      (index, PersonStatus(prefList))
    }
  }

  /**
   * Print for debug.
   */
  def printStatus(num: Int) = {
    println("Proposers:")
    printPersonStatus(proposers, num)
    println("Acceptors:")
    printPersonStatus(acceptors, num)
  }

  private def printPersonStatus(status: RDD[(Index, PersonStatus)], num: Int) = {
    status.take(num)
      .map( item => item._1.toString + ": " + item._2.fiance + ", " + item._2.list.mkString(" ") )
      .foreach(println)
  }
}

