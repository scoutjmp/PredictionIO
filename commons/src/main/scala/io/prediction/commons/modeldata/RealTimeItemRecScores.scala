package io.prediction.commons.modeldata

import io.prediction.commons.settings.{ Algo, App, OfflineEval }

import com.github.nscala_time.time.Imports._

/**
 * ItemRecScore object for real time recommendation
 * This object represents an item to be recommended to a user.
 *
 * @param uid User ID.
 * @param iid Item ID.
 * @param score Recommendation score.
 * @param time The item time.
 * @param itypes Item types of the item recommended. Copied from the item when a batch mode algorithm is run.
 * @param appid App ID of this record.
 * @param algoid Algo ID of this record.
 * @param modelset Model data set.
 * @param id ItemRecScore ID (optional field used internally for sorting)
 */
case class RealTimeItemRecScore(
  uid: String,
  iid: String,
  score: Double,
  time: DateTime,
  itypes: Seq[String],
  appid: Int,
  algoid: Int,
  id: Option[String] = None)

/** Base trait for implementations that interact with itemrec scores in the backend data store. */
trait RealTimeItemRecScores {
  /** Save(create new one or update existing) an ItemRecScore and return it with a real ID, if any (database vendor dependent). */
  def save(itemRecScore: RealTimeItemRecScore): RealTimeItemRecScore

  /**
   * Get the top N RealTimeItemRecScores ranked by score in descending order.
   *
   * @param after Returns the next top N results after the provided ItemRecScore, if provided.
   */
  def getTopN(uid: String, n: Int, itypes: Option[Seq[String]], after: Option[RealTimeItemRecScore])(implicit app: App, algo: Algo, offlineEval: Option[OfflineEval] = None): Iterator[RealTimeItemRecScore]

  /** Delete by Algo ID. */
  def deleteByAlgoid(algoid: Int)

  /** Delete by Algo ID and model set. */
  //def deleteByAlgoidAndModelset(algoid: Int, modelset: Boolean)

  /** Delete old RealTimeItemRecScores by Algo ID and time (less than or equal to the time) */
  def deleteOldByAlgoidAndTime(algoid: Int, time: DateTime)

  /** Check whether data exist for a given Algo. */
  def existByAlgo(algo: Algo): Boolean
}
