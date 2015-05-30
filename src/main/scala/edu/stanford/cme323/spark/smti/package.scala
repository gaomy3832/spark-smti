package edu.stanford.cme323.spark

package object smti {
  type Rank = Long
  type Index = Long

  val InvIndex: Index = -1

  /* One preference: rank and index. */
  case class Pref (val rank: Rank = Long.MaxValue, val index: Index = InvIndex)

  /* Preference list for one person. */
  type PrefList = Array[Pref]

  /* Status for one person. */
  case class PersonStatus (val list: PrefList = Array.empty, var fiance: Index = InvIndex)
}
