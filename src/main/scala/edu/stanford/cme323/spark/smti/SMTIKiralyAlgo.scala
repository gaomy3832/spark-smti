package edu.stanford.cme323.spark.smti

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


private[smti] case class PropStatus (
  val listPos: Int = 0
)

private[smti] case class AccpStatus (
  val flighty: Boolean = false
)

class SMTIKiralyAlgo (propPrefList: RDD[(Index, PrefList)], accpPrefList: RDD[(Index, PrefList)])
  extends SMTI[PropStatus, AccpStatus](propPrefList, accpPrefList,
    new PropStatus(), new AccpStatus()) {

  def run() {

    var activeProposers: Long = proposers.count()
    var round: Int = 0

    while (activeProposers > 0) {

      // Proposers make proposal to the top person in the list
      // Proposals are grouped by acceptors
      case class Proposal(val propIdx: Index, val uncertain: Boolean)

      val proposals: RDD[(Index, Iterable[Proposal])] =
        proposers
          .filter{ case(selfIndex, person) =>
            person.status.listPos < 2 * person.prefList.size && person.fiance == InvIndex }
          .map{ case(selfIndex, person) =>
            val listPos = person.status.listPos % person.prefList.size
            val (favoriteIndex, uncertain) = person.prefList.getFavorite(listPos)
            (favoriteIndex, Proposal(selfIndex, uncertain))
          }
          .groupByKey()

      activeProposers = proposals.count()
      logInfo(f"Round $round%3d: Has $activeProposers%5d active proposers")
      round += 1

      // Acceptors deal with proposals
      acceptors =
        acceptors
          .leftOuterJoin(proposals)
          .mapValues{ case(person, propGroup) =>
            if (propGroup.isEmpty) {
              // no proposal received
              person
            } else {
              // select the best proposal
              val bestProp = propGroup.get.maxBy( prop => person.prefList.getRankOf(prop.propIdx) )
              if (person.fiance == InvIndex || person.status.flighty ||
                person.prefList.getRankOf(bestProp.propIdx) < person.prefList.getRankOf(person.fiance)) {
                // accept
                new Person(person.prefList, bestProp.propIdx, AccpStatus(bestProp.uncertain))
              } else {
                person
              }
            }
          }

      // Acceptors respond with their current fiances
      // Responses are grouped by proposers
      case class Response(val accpIdx: Index)

      val responses: RDD[(Index, Response)] =
        acceptors
          .map{ case(selfIndex, person) =>
            (person.fiance, Response(selfIndex))
          }

      // Proposers update themselves based on responses
      proposers =
        proposers
          .leftOuterJoin(responses)
          .mapValues{ case(person, resp) =>
            if (resp.isEmpty) {
              // no response
              var listPos = person.status.listPos
              if (person.fiance != InvIndex) {
                // break up with previous fiance
                // check if uncertain
                val uncertain = person.prefList.getFavorite(person.status.listPos)._2
                if (uncertain) listPos += 1
              } else {
                // proposing fails
                listPos += 1
              }
              new Person(person.prefList, InvIndex, PropStatus(listPos))
            } else {
              if (person.fiance != InvIndex) {
                // keep relationship
                assert(person.fiance == resp.get.accpIdx)
                person
              } else {
                // proposing succeeds
                new Person(person.prefList, resp.get.accpIdx, PropStatus(person.status.listPos))
              }
            }
          }
    }

  }
}

