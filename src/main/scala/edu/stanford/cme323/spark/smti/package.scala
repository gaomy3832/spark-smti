package edu.stanford.cme323.spark


package object smti {
  type Rank = Long
  type Index = Long

  val InvIndex: Index = -1
  val LastRank: Rank = Long.MaxValue

  /* One preference: rank and index. */
  case class Pref (val rank: Rank = LastRank, val index: Index = InvIndex)

  /* Preference list for one person. */
  type PrefList = Array[Pref]

  /* Operations on preference list. */
  implicit class PrefListOps (protected val list: PrefList) {

    // Check if _index_ is in the list
    def containsIndex(index: Index): Boolean = {
      list.map(_.index).contains(index)
    }

    // Get the rank of _index_ in the list
    def getRankOf(index: Index): Rank = {
      list.find( pref => pref.index == index ).getOrElse(Pref()).rank
    }

  }
}
