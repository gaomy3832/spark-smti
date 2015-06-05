package edu.stanford.cme323.spark.smti.utils

import java.io.File

import scala.util.Random

import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import edu.stanford.cme323.spark.smti._


object DataGen extends Logging {

  def generate(sc: SparkContext, n: Long, pi: Double, pt: Double, seed: Int):
      (RDD[(Index, PrefList)], RDD[(Index, PrefList)]) =
  {
    var men: RDD[(Index, PrefList)] = null
    var women: RDD[(Index, PrefList)] = null

    val seedGen = new Random(seed)

    logInfo(f"Generate random preference lists with n = $n, Pi = $pi, Pt = $pt, seed = $seed")

    // incomplete
    val s1 = seedGen.nextInt()

    var mlists: RDD[(Index, List[Index])] = null
    var wlists: RDD[(Index, List[Index])] = null
    var anyEmpty: Boolean = true

    do {
      mlists = sc.parallelize(1L to n)
        .map{ idx =>
          val rnd = new Random(s1 + idx)
          val list = rnd.shuffle((1L to n).toList)
            .filter( x => rnd.nextDouble() > pi )
          (idx, list)
        }

      wlists = mlists
        .flatMap( kv => kv._2.map( v => (v, kv._1) ) )
        .groupByKey()
        .mapValues( iter => iter.toList )

      anyEmpty = (mlists union wlists).filter( kv => kv._2.isEmpty ).count() != 0

    } while (anyEmpty)

    // ties
    val s2 = seedGen.nextInt()

    men = mlists
      .map{ case(idx, list) =>
        val rankList = makeRankList(list.length, pt, (s2 + idx).toInt)
        val prefList = (rankList zip list).map( tuple => Pref(tuple._1, tuple._2) )
        (idx, prefList)
      }

    women = wlists
      .map{ case(idx, list) =>
        val rankList = makeRankList(list.length, pt, (s2 + idx).toInt)
        val prefList = (rankList zip list).map( tuple => Pref(tuple._1, tuple._2) )
        (idx, prefList)
      }

    // stats
    val mstats = stat(men)
    logInfo("Men: average list length: " + mstats._1 + ", average number of ties: " + mstats._2)
    val wstats = stat(women)
    logInfo("Women: average list length: " + mstats._1 + ", average number of ties: " + mstats._2)

    (men, women)
  }

  private def makeRankList(len: Int, pt: Double, rankSeed: Int): Array[Rank] = {
    val rnd = new Random(rankSeed)
    var rankList = new Array[Rank](len)
    rankList(0) = 1
    for ( i <- 1 until len ) {
      if (rnd.nextDouble() < pt) {
        rankList(i) = rankList(i-1)
      } else {
        rankList(i) = rankList(i-1) + 1
      }
    }
    rankList
  }

  private def stat(people: RDD[(Index, PrefList)]): (Double, Double) = {
    val stats = people
      .map{ case(idx, prefList) =>
        val len = prefList.length
        val ties = len - prefList.last.rank
        (len, ties, 1)
      }
      .fold((0, 0, 0))( (x, y) => (x._1+y._1, x._2+y._2, x._3+y._3) )
    (stats._1.toDouble / stats._3, stats._2.toDouble / stats._3)
  }

}

