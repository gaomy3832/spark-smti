package edu.stanford.cme323.spark.smti

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging


private[smti] class Person[Status] (
  val prefList: PrefList,
  val fiance: Index,
  val status: Status
) extends Serializable

abstract class SMTI[PropStatus, AccpStatus] (
    propPrefList: RDD[(Index, PrefList)],
    accpPrefList: RDD[(Index, PrefList)],
    initPropSt: PropStatus,
    initAccpSt: AccpStatus)
  extends Serializable with Logging {

  protected var proposers: RDD[(Index, Person[PropStatus])] =
    propPrefList.mapValues( prefList => new Person(prefList, InvIndex, initPropSt) )
  protected var acceptors: RDD[(Index, Person[AccpStatus])] =
    accpPrefList.mapValues( prefList => new Person(prefList, InvIndex, initAccpSt) )

  def run()

  def results(): RDD[(Index, Index)] = {
    proposers.mapValues( person => person.fiance )
  }

  def verify(): Boolean = {
    val res = results()

    // check whether fiance in prefList, or single
    val propFianceInvalid =
      res.join(proposers.mapValues( person => person.prefList ))
        .mapValues{ case(fiance, prefList) =>
          fiance != InvIndex && !prefList.contains(fiance)
        }
        .filter( kv => kv._2 )
        .count()
    val accpFianceInvalid =
      res.map( kv => (kv._2, kv._1) )
        .join(acceptors.mapValues( person => person.prefList ))
        .mapValues{ case(fiance, prefList) =>
          fiance != InvIndex && !prefList.contains(fiance)
        }
        .filter( kv => kv._2 )
        .count()

    // check the relationship info in two sides matches
    val validPropRelations = res
      .filter( kv => kv._2 != InvIndex )
    val validAccpRelations = acceptors.mapValues( person => person.fiance )
      .filter( kv => kv._2 != InvIndex )
    val mismatches = validPropRelations
      .fullOuterJoin(validAccpRelations.map( kv => (kv._2, kv._1) ))
      .mapValues{ case(option1, option2) =>
        option1.isEmpty || option2.isEmpty || option1.get != option2.get
      }
      .filter( kv => kv._2 )
      .count()

    propFianceInvalid == 0 && accpFianceInvalid == 0 && mismatches == 0
  }

  def sizeOfMarriage(): Long = {
    results().filter( kv => kv._2 != InvIndex ).count()
  }

}

