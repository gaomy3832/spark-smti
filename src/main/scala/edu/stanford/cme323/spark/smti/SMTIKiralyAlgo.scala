package edu.stanford.cme323.spark.smti

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


private[smti] case class PropStatus (
  val uncertain: Boolean = false,
  val listPos: Int = 0
) {
  def updateList(offset: Int): PropStatus = new PropStatus(uncertain, listPos + offset)
}

private[smti] case class AccpStatus (
  val flighty: Boolean = false
)

class SMTIKiralyAlgo (propPrefList: RDD[(Index, PrefList)], accpPrefList: RDD[(Index, PrefList)])
  extends SMTI[PropStatus, AccpStatus](propPrefList, accpPrefList,
    new PropStatus(), new AccpStatus()) {

  def run() {}

}

