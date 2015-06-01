package edu.stanford.cme323.spark.smti


/* One preference: rank and index. */
case class Pref (val rank: Rank = LastRank, val index: Index = InvIndex)


/* Preference list for one person. */
class PrefList (private val list: Array[Pref]) extends Serializable {

  val size = list.length

  def at(pos: Int): Pref = {
    list(pos)
  }

  def contains(index: Index): Boolean = {
    !list.find( pref => pref.index == index ).isEmpty
  }

  def getRankOf(index: Index): Rank = {
    list.find( pref => pref.index == index ).getOrElse(Pref()).rank
  }

  def getFavorite(pos: Int): (Index, Boolean) = {
    var nextRankPos = pos
    val rank = list(pos).rank
    while (list(nextRankPos).rank == rank) nextRankPos += 1
    // FIXME: random select
    (list(pos).index, pos + 1 == nextRankPos)
  }
}
