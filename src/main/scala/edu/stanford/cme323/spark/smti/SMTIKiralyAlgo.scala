package edu.stanford.cme323.spark.smti

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


private[smti] case class PropStatus (
  val listPos: Int = 0
)

private[smti] case class AccpStatus (
  val flighty: Boolean = false
)

private[smti] case class Proposal(
  val from: Index,
  val uncertain: Boolean
)

private[smti] case class Response(
  val from: Index
)

class SMTIKiralyAlgo (propPrefList: RDD[(Index, PrefList)], accpPrefList: RDD[(Index, PrefList)])
  extends SMTI[PropStatus, AccpStatus](propPrefList, accpPrefList,
    new PropStatus(), new AccpStatus()) {


  def run(maxRounds: Int) {
    run(maxRounds,
      propActive,
      propMakeProposal,
      accpMakeResponse,
      propHandleResponse,
      accpHandleProposal)
  }



  private def propActive(person: Proposer): Boolean = {
    person.status.listPos < 2 * person.prefList.size && person.fiance == InvIndex
  }

  private def propMakeProposal(selfIdx: Index, person: Proposer): (Index, Proposal) = {
    val listPos = person.status.listPos % person.prefList.size
    val (favoriteIndex, uncertain) = person.prefList.getFavorite(listPos)
    (favoriteIndex, Proposal(selfIdx, uncertain))
  }

  private def accpMakeResponse(selfIdx: Index, person: Acceptor): (Index, Response) = {
    (person.fiance, Response(selfIdx))
  }

  private def propHandleResponse(person: Proposer, optn: Option[Response]): Proposer = {
    var listPos = person.status.listPos
    if (optn.isEmpty) {
      // no response received
      if (person.fiance != InvIndex) {
        // break up with previous fiance, only remove from list if uncertain
        val uncertain = person.prefList.getFavorite(person.status.listPos)._2
        if (uncertain) listPos += 1
      } else {
        // proposing fails
        listPos += 1
      }
      new Person(person.prefList, InvIndex, PropStatus(listPos))
    } else {
      val resp = optn.get
      // keep relationship or proposing succeeds
      assert(person.fiance == resp.from || person.fiance == InvIndex)
      new Person(person.prefList, resp.from, PropStatus(listPos))
    }
  }

  private def accpHandleProposal(person: Acceptor, optn: Option[Iterable[Proposal]]): Acceptor = {
    if (optn.isEmpty) {
      // no proposal received
      person
    } else {
      val iter = optn.get
      // select the best proposal
      val bestProp = iter.minBy( prop => person.prefList.getRankOf(prop.from) )
      if (person.fiance == InvIndex || person.status.flighty ||
          person.prefList.getRankOf(bestProp.from) < person.prefList.getRankOf(person.fiance)) {
        // accept
        new Acceptor(person.prefList, bestProp.from, AccpStatus(bestProp.uncertain))
      } else {
        person
      }
    }
  }
}

