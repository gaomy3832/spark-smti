package edu.stanford.cme323.spark.smti

import org.apache.spark.rdd.RDD


/* Status for one person. */
private case class PersonStatus (val list: PrefList = Array.empty, var fiance: Index = InvIndex)

class SMTI (propPrefList: RDD[(Index, PrefList)], accpPrefList: RDD[(Index, PrefList)]) {

  private var proposers: RDD[(Index, PersonStatus)] = propPrefList.map( kv => (kv._1, PersonStatus(kv._2)) )
  private var acceptors: RDD[(Index, PersonStatus)] = accpPrefList.map( kv => (kv._1, PersonStatus(kv._2)) )



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

