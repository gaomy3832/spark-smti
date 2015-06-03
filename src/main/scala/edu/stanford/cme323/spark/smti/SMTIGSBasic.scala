package edu.stanford.cme323.spark.smti

import org.apache.spark.rdd.RDD


private[smti] case class BasicProp (
  val listPos: Int = 0
)

private[smti] case class BasicAccp (
  val curRank: Rank = LastRank
)


class SMTIGSBasic (
    propPrefList: RDD[(Index, PrefList)],
    accpPrefList: RDD[(Index, PrefList)])
  extends SMTIGS[BasicProp, BasicAccp](
    propPrefList, accpPrefList,
    new BasicProp(), new BasicAccp())
{

  private case class Proposal(val from: Index)
  private case class Response(val from: Index)

  def run(maxRounds: Int = Int.MaxValue) {

    def propMakeProposal = (selfIdx: Index, person: Proposer) => {
      if (person.status.listPos < person.prefList.length && person.fiance == InvIndex) {
        val listPos = person.status.listPos % person.prefList.length
        val favoriteIndex = person.prefList(listPos).index
        List((favoriteIndex, Proposal(selfIdx)))
      } else {
        List.empty
      }
    }

    def accpMakeResponse = (selfIdx: Index, person: Acceptor) => {
      List((person.fiance, Response(selfIdx)))
    }

    def propHandleResponse = (person: Proposer, optn: Option[Iterable[Response]]) => {
      var listPos = person.status.listPos
      if (optn.isEmpty) {
        // no response received
        // break up with previous fiance, or proposing fails
        listPos += 1
        new Person(person.prefList, InvIndex, BasicProp(listPos))
      } else {
        val iter = optn.get
        assert(iter.size == 1)
        val resp = iter.head
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
      propMakeProposal,
      accpMakeResponse,
      propHandleResponse,
      accpHandleProposal)
  }

}

