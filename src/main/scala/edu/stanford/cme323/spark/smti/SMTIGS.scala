package edu.stanford.cme323.spark.smti

import java.io.File

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD


private[smti] class Person[Status] (
  val prefList: PrefList,
  val fiance: Index,
  val status: Status
) extends Serializable


/* SMTI using GS Scheme base class. */
abstract class SMTIGS[PropStatus, AccpStatus] (
    propPrefList: RDD[(Index, PrefList)],
    accpPrefList: RDD[(Index, PrefList)],
    initPropSt: PropStatus,
    initAccpSt: AccpStatus)
  extends Serializable with Logging {

  protected type Proposer = Person[PropStatus]
  protected type Acceptor = Person[AccpStatus]


  /**
   * Data members.
   */
  protected var proposers: RDD[(Index, Proposer)] =
    propPrefList.mapValues( prefList => new Proposer(prefList, InvIndex, initPropSt) )
  protected var acceptors: RDD[(Index, Acceptor)] =
    accpPrefList.mapValues( prefList => new Acceptor(prefList, InvIndex, initAccpSt) )

  private val checkpointDir = File.createTempFile(".cp_dir", "")
  checkpointDir.delete()
  proposers.sparkContext.setCheckpointDir(checkpointDir.toString)
  acceptors.sparkContext.setCheckpointDir(checkpointDir.toString)


  /**
   * Algorithm execution template.
   */
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

      /* Proposers to acceptors. */
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

      /* Acceptors to proposers. */
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

      /* Break the long RDD lineage to avoid stackoverflow error. */
      if (round % 30 == 0) {
        proposers.checkpoint()
        acceptors.checkpoint()
      }

      /* Iteration metadata. */
      numActiveProposals =
        proposals
          .map{ case(key, iter) => iter.size }
          .fold(0)(_ + _) // equv. to reduce(_ + _) but also handles empty RDD
      logInfo(f"Round $round%3d: Has $numActiveProposals%5d active proposers")
      round += 1

      prevProposers.unpersist(blocking=false)
      prevAcceptors.unpersist(blocking=false)
      proposals.unpersist(blocking=false)
      responses.unpersist(blocking=false)

    } while (numActiveProposals > 0 && round < maxRounds)

  }


  /**
   * Results and verification.
   */
  def rawResults(): RDD[(Index, Index)] = {
    proposers.mapValues( person => person.fiance )
  }

  def marriage(): RDD[(Index, Index)] = {
    rawResults().filter( pair => pair._2 != InvIndex )
  }

  def verify(): Boolean = {
    if (!sanityCheck()) {
      logError("sanity check fails!")
      false
    } else if (!stableCheck()) {
      logError("stable check fails!")
      false
    } else {
      true
    }
  }

  def sanityCheck(): Boolean = {
    val res = rawResults()

    // check whether fiance in prefList, or single
    val propFianceInvalid =
      proposers
        .mapValues( person => person.prefList )
        .join(res)
        .mapValues{ case(prefList, fiance) =>
          fiance != InvIndex && !prefList.contains(fiance)
        }
        .filter( kv => kv._2 )
        .count()
    val accpFianceInvalid =
      acceptors
        .mapValues( person => person.prefList )
        .join(res.map( kv => (kv._2, kv._1) ))
        .mapValues{ case(prefList, fiance) =>
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

  def stableCheck(): Boolean = {
    // people who are more preferred by the acceptors than their fiances
    // Grouped by proposers
    val accpMorePreferred =
      acceptors
        .flatMapValues{ person =>
          val curRank = person.prefList.getRankOf(person.fiance)
          var pos = 0
          var morePreferred = List[Index]()
          while (pos < person.prefList.size && person.prefList.at(pos).rank < curRank) {
            morePreferred = person.prefList.at(pos).index :: morePreferred
            pos += 1
          }
          morePreferred
        }
        .map( pair => (pair._2, pair._1) )

    val blockPairs =
      proposers
        .join(accpMorePreferred)
        .flatMapValues{ case(person, prop) =>
          if (person.prefList.getRankOf(prop) < person.prefList.getRankOf(person.fiance)) {
            List(prop)
          } else {
            List[Index]()
          }
        }

    blockPairs.count() == 0
  }

}

