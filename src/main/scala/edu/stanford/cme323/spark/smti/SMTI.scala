package edu.stanford.cme323.spark.smti

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging

import scala.reflect._


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

  protected type Proposer = Person[PropStatus]
  protected type Acceptor = Person[AccpStatus]

  protected var proposers: RDD[(Index, Proposer)] =
    propPrefList.mapValues( prefList => new Proposer(prefList, InvIndex, initPropSt) )
  protected var acceptors: RDD[(Index, Acceptor)] =
    accpPrefList.mapValues( prefList => new Acceptor(prefList, InvIndex, initAccpSt) )


  protected def doMatching[Proposal: ClassTag, Response: ClassTag]
      (maxRounds: Int,
       propActive: Proposer => Boolean,
       propMakeProposal: (Index, Proposer) => (Index, Proposal),
       accpMakeResponse: (Index, Acceptor) => (Index, Response),
       propHandleResponse: (Proposer, Option[Response]) => Proposer,
       accpHandleProposal: (Acceptor, Option[Iterable[Proposal]]) => Acceptor)
  {

    var numActiveProposals: Long = 0
    var round: Int = 0

    var prevProposers: RDD[(Index, Proposer)] = null
    var prevAcceptors: RDD[(Index, Acceptor)] = null

    do {

      prevProposers = proposers
      prevAcceptors = acceptors

      /**
       * Proposers to acceptors.
       */
      // Proposers make proposals
      // Proposals are grouped by acceptors
      val proposals: RDD[(Index, Iterable[Proposal])] =
        proposers
          .filter( kv => propActive(kv._2) )
          .map( kv => propMakeProposal(kv._1, kv._2) )
          .groupByKey()
          .cache()

      // Acceptors handle proposals
      acceptors =
        acceptors
          .leftOuterJoin(proposals)
          .mapValues( pair => accpHandleProposal(pair._1, pair._2) )
          .cache()

      /**
       * Acceptors to proposers.
       */
      // Acceptors respond with their current fiances
      // Responses are grouped by proposers
      val responses: RDD[(Index, Response)] =
        acceptors
          .map( kv => accpMakeResponse(kv._1, kv._2) )
          .cache()

      // Proposers update themselves based on responses
      proposers =
        proposers
          .leftOuterJoin(responses)
          .mapValues( pair => propHandleResponse(pair._1, pair._2) )
          .cache()

      /**
       * Iteration metadata.
       */
      numActiveProposals =
        proposals
          .map{ case(key, iter) => iter.size }
          .reduce(_ + _)
      logInfo(f"Round $round%3d: Has $numActiveProposals%5d active proposers")
      round += 1

      prevProposers.unpersist(blocking=false)
      prevAcceptors.unpersist(blocking=false)
      proposals.unpersist(blocking=false)
      responses.unpersist(blocking=false)

    } while (numActiveProposals > 0 && round < maxRounds)

  }


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

