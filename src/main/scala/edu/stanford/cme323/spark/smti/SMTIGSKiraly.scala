package edu.stanford.cme323.spark.smti

import org.apache.spark.rdd.RDD


// Uncertain means although the proposer engages with the current favorite,
// there are still other single candidates (whom the proposer will then prefer
// to the current fiance.
//
// Proposer can be inactive, single, uncertain engaged, and engaged.  When he
// is inactive, he does not propose any more; when he is single, he proposes to
// the next favorite acceptor; when he is uncertain engaged, he does not
// propose until becoming not uncertain; when he is engaged, he keeps proposing
// to his current fiance.  Acceptor chooses the best in received proposals, and
// always replaces the current fiance.
//
// Proposer can also be in the 1st pass or 2nd pass for his preference list.
// Acceptor prefers 2nd-pass proposer to 1st-pass proposer.
//
// If acceptor is engaged, she only responds to the current fiance; otherwise
// she announce to all proposers in her list about her singleness by using the
// negative index.

private[smti] case class KiralyProp (
  // Current candidates with same rank, from the preference lsit
  // single candidates are in front of engaged candidates
  val candidates: List[Index] = List.empty,
  // The starting position for next group of candidates
  val nextCandPos: Int = 0,
  // A list containing the acceptors who are single now, and thus are more
  // preferred by the proposer
  val singleAcceptors: List[Index] = List.empty
)

private[smti] case class KiralyAccp ()


class SMTIGSKiraly (
    propPrefList: RDD[(Index, PrefList)],
    accpPrefList: RDD[(Index, PrefList)])
  extends SMTIGS[KiralyProp, KiralyAccp](
    propPrefList, accpPrefList,
    new KiralyProp(), new KiralyAccp())
{

  private case class Proposal(
    val from: Index
  )

  private case class Response(
    val from: Index
  )

  implicit class KiralyPrefListOps (list: PrefList) extends PrefListOps(list) {

    // Get all candidates with the same highest rank after given _pos_
    def getCandsAfter(pos: Int): List[Index] = {
      var cands: List[Index] = List.empty
      val rank = list(pos).rank
      var i = pos
      while (i < list.length && list(i).rank == rank) {
        cands = list(i).index :: cands
        i += 1
      }
      cands
    }

  }

  override def isActive(person: Proposer): Boolean = {
    person.status.nextCandPos < 2 * person.prefList.length
  }

  override def run(maxRounds: Int = Int.MaxValue, numPartitions: Int = 2) {

    def propMakeProposal = (selfIdx: Index, person: Proposer) => {
      if (isActive(person)) {
        assert(!person.status.candidates.isEmpty)
        if (person.fiance == InvIndex) {
          // single state
          List((person.status.candidates(0), Proposal(selfIdx)))
        } else {
          if (person.status.singleAcceptors.contains(person.status.candidates(0))) {
            // uncertain engaged
            List.empty
          } else {
            // engaged
            List((person.fiance, Proposal(selfIdx)))
          }
        }
      } else {
        // inactive
        List.empty
      }
    }

    def accpMakeResponse = (selfIdx: Index, person: Acceptor) => {
      if (person.fiance != InvIndex) {
        List((person.fiance, Response(selfIdx)))
      } else {
        person.prefList.map( pref => (pref.index, Response(-selfIdx)) ).toList
      }
    }

    def propHandleResponse = (person: Proposer, optn: Option[Iterable[Response]]) => {
      val respList: List[Response] = optn.getOrElse(Iterator.empty).toList
      val engagedAcceptors = respList.filter(_.from > 0).map(_.from)
      val singleAcceptors = respList.filter(_.from < 0).map(-_.from)

      var fiance = InvIndex
      var candidates = person.status.candidates
      var nextCandPos = person.status.nextCandPos

      if (engagedAcceptors.isEmpty) {
        if (person.fiance != InvIndex) {
          // break up with previous fiance
          assert(candidates.contains(person.fiance))
          assert(!singleAcceptors.contains(person.fiance))
          val notUncertain = candidates.intersect(singleAcceptors).isEmpty
          if (notUncertain) {
            candidates = candidates.diff(List(person.fiance))
          }
        } else {
          // proposing fails
          candidates = candidates.drop(1)
        }
      } else {
        assert(engagedAcceptors.length == 1)
        fiance = engagedAcceptors.head
        // keep relationship or proposing succeeds
        assert(person.fiance == fiance || person.fiance == InvIndex)
      }

      if (candidates.isEmpty) {
        candidates = person.prefList.getCandsAfter(nextCandPos % person.prefList.length)
        nextCandPos += candidates.length
      }

      // Make sure single candidates have higher priority (at the beginning of the candidates)
      val singleCands = candidates.intersect(singleAcceptors)
      candidates = singleCands ::: candidates.diff(singleCands)

      new Proposer(person.prefList, fiance, KiralyProp(candidates, nextCandPos, singleAcceptors))
    }

    def accpHandleProposal = (person: Acceptor, optn: Option[Iterable[Proposal]]) => {
      if (optn.isEmpty) {
        // no proposal received
        person
      } else {
        // select the best proposal
        val bestProp = optn.get.minBy( prop => person.prefList.getRankOf(prop.from) )
        new Acceptor(person.prefList, bestProp.from, KiralyAccp())
      }
    }

    def initProposer = (person: Proposer) => {
      val cands = person.prefList.getCandsAfter(0)
      new Proposer(person.prefList, InvIndex, KiralyProp(cands, cands.length,
        person.prefList.map(_.index).toList))
    }

    doMatching(maxRounds,
      numPartitions,
      propMakeProposal,
      accpMakeResponse,
      propHandleResponse,
      accpHandleProposal,
      initProposer)
  }

}

