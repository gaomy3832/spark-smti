package edu.stanford.cme323.spark.smti

import org.apache.spark.rdd.RDD


private[smti] case class BasicProp (
  val listPos: Int = 0
)

private[smti] case class BasicAccp (
  val curRank: Rank = LastRank
)


class SMTIGSBasic (propPrefList: RDD[(Index, PrefList)], accpPrefList: RDD[(Index, PrefList)])
  extends SMTIGS[BasicProp, BasicAccp](propPrefList, accpPrefList,
    new BasicProp(), new BasicAccp()) {

  private case class Proposal(
    val from: Index
  )

  private case class Response(
    val from: Index
  )

  def run(maxRounds: Int = Int.MaxValue) {

    def propActive = (person: Proposer) => {
      person.status.listPos < person.prefList.size && person.fiance == InvIndex
    }

    def propMakeProposal = (selfIdx: Index, person: Proposer) => {
      val listPos = person.status.listPos % person.prefList.size
      val favoriteIndex = person.prefList.getFavorite(listPos)._1
      (favoriteIndex, Proposal(selfIdx))
    }

    def accpMakeResponse = (selfIdx: Index, person: Acceptor) => {
      (person.fiance, Response(selfIdx))
    }

    def propHandleResponse = (person: Proposer, optn: Option[Response]) => {
      var listPos = person.status.listPos
      if (optn.isEmpty) {
        // no response received
        // break up with previous fiance, or proposing fails
        listPos += 1
        new Person(person.prefList, InvIndex, BasicProp(listPos))
      } else {
        val resp = optn.get
        // keep relationship or proposing succeeds
        assert(person.fiance == resp.from || person.fiance == InvIndex)
        new Person(person.prefList, resp.from, BasicProp(listPos))
      }
    }

    def accpHandleProposal = (person: Acceptor, optn: Option[Iterable[Proposal]]) => {
      if (optn.isEmpty) {
        // no proposal received
        person
      } else {
        val iter = optn.get
        // select the best proposal
        val bestProp = iter.minBy( prop => person.prefList.getRankOf(prop.from) )
        val newRank = person.prefList.getRankOf(bestProp.from)
        if (newRank < person.status.curRank) {
          // accept
          new Acceptor(person.prefList, bestProp.from, BasicAccp(newRank))
        } else {
          person
        }
      }
    }

    doMatching(maxRounds,
      propActive,
      propMakeProposal,
      accpMakeResponse,
      propHandleResponse,
      accpHandleProposal)
  }

}

