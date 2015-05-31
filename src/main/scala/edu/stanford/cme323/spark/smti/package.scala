package edu.stanford.cme323.spark

import org.apache.log4j.Logger


package object smti {
  type Rank = Long
  type Index = Long

  val InvIndex: Index = -1
  val LastRank: Rank = Long.MaxValue

  val logger = Logger.getLogger("SMTI")
}
