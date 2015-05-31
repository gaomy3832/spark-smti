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

  def run() {

    var activeProposers = proposers.count()

    //while () {

      // Proposers make proposal to the top person in the list
      // Proposals are grouped by acceptors
      val proposals: RDD[(Index, Iterable[(Index, Boolean)])] =
        proposers
          .filter{ case(selfIndex, person) =>
            person.status.listPos < 2 * person.prefList.size && person.fiance == InvIndex }
          .map{ case(selfIndex, person) =>
            val listPos = person.status.listPos % person.prefList.size
            val favoriteIndex = person.prefList.at(listPos).index
            val uncertain = person.status.uncertain
            (favoriteIndex, (selfIndex, uncertain))
          }
          .groupByKey()

      activeProposers = proposals.count()

      // Remove the top person from the proposer lists
      proposers = proposers
        .mapValues{ person =>
          new Person(person.prefList, person.fiance, person.status.updateList(1))
        }

      // Acceptors deal with proposals and make acceptance (and rejection to previous fiance)
      // Answer is an array of length 2, first is new fiance, last is old fiance
      // Answers are grouped by acceptors
      val answers: RDD[(Index, Array[Index])] =
        acceptors
          .join(proposals)
          .mapValues{ case(person, propGroup) =>
            val bestProp = propGroup.maxBy( prop => person.prefList.getRankOf(prop._1) )
            (person, bestProp)
          }
          .filter{ kv =>
            val person = kv._2._1
            val bestProp = kv._2._2
            person.fiance == InvIndex ||
            person.status.flighty ||
            person.prefList.getRankOf(bestProp._1) < person.prefList.getRankOf(person.fiance)
          }
          .mapValues{ case(person, bestProp) =>
            Array(bestProp._1, person.fiance)
          }

      //answers.mapValues( array => array.mkString(" ") ).take(10).foreach(println)

      acceptors = acceptors
        .leftOuterJoin(answers)
        .mapValues{ case(person, answer) =>
          if (answer.isEmpty) {
            person
          } else {
            // TODO: update flighty status here
            new Person(person.prefList, answer.get.head, person.status)
          }
        }

      //val answersToProposers = answers
        //.flatMap{ case(accpIndex, array) =>
          //Array((array.head, accpIndex), (array))
        //}

      //proposers = proposers
        //.leftOuterJoin(answers.flatMap( case(index, array) => array.map( ) ))


    //}

    proposers
      .mapValues{ person =>
        person.status.listPos
      }
      .take(10)
      .foreach(println)

    acceptors
      .mapValues{ person =>
        person.fiance
      }
      .take(10)
      .foreach(println)


  }
}

