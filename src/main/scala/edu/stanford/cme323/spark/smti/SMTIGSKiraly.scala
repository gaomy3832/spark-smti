package edu.stanford.cme323.spark.smti

import org.apache.spark.rdd.RDD


private[smti] case class KiralyProp (
  val listPos: Int = 0
)

private[smti] case class KiralyAccp (
  val flighty: Boolean = false
)


class SMTIGSKiraly (
    propPrefList: RDD[(Index, PrefList)],
    accpPrefList: RDD[(Index, PrefList)])
  extends SMTIGS[KiralyProp, KiralyAccp](
    propPrefList, accpPrefList,
    new KiralyProp(), new KiralyAccp())
{

  private case class Proposal(
    val from: Index,
    val uncertain: Boolean
  )

  private case class Response(
    val from: Index
  )

  implicit class KiralyPrefListOps (list: PrefList) extends PrefListOps(list) {
    def getFavorite(pos: Int): (Index, Boolean) = {
      var nextRankPos = pos
      val rank = list(pos).rank
      while (nextRankPos < list.length && list(nextRankPos).rank == rank) nextRankPos += 1
      // FIXME: random select
      (list(pos).index, pos + 1 == nextRankPos)
    }
  }

  def run(maxRounds: Int = Int.MaxValue) {

    def propActive = (person: Proposer) => {
      person.status.listPos < 2 * person.prefList.length && person.fiance == InvIndex
    }

    def propMakeProposal = (selfIdx: Index, person: Proposer) => {
      val listPos = person.status.listPos % person.prefList.length
      val (favoriteIndex, uncertain) = person.prefList.getFavorite(listPos)
      (favoriteIndex, Proposal(selfIdx, uncertain))
    }

    def accpMakeResponse = (selfIdx: Index, person: Acceptor) => {
      (person.fiance, Response(selfIdx))
    }

    def propHandleResponse = (person: Proposer, optn: Option[Response]) => {
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
          new Person(person.prefList, InvIndex, KiralyProp(listPos))
        } else {
          val resp = optn.get
          // keep relationship or proposing succeeds
          assert(person.fiance == resp.from || person.fiance == InvIndex)
          new Person(person.prefList, resp.from, KiralyProp(listPos))
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
        if (person.fiance == InvIndex || person.status.flighty ||
          person.prefList.getRankOf(bestProp.from) < person.prefList.getRankOf(person.fiance)) {
            // accept
            new Acceptor(person.prefList, bestProp.from, KiralyAccp(bestProp.uncertain))
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

