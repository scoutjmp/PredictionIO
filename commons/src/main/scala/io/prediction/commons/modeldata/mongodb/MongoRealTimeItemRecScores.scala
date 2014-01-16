package io.prediction.commons.modeldata.mongodb

import io.prediction.commons.MongoUtils._
import io.prediction.commons.modeldata.{ RealTimeItemRecScore, RealTimeItemRecScores }
import io.prediction.commons.settings.{ Algo, App, OfflineEval }

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.conversions.scala._
import com.github.nscala_time.time.Imports._

/** MongoDB implementation of RealTimeItemRecScores. */
class MongoRealTimeItemRecScores(db: MongoDB) extends RealTimeItemRecScores {
  private val itemRecScoreColl = db("realtime_itemRecScores")

  RegisterJodaTimeConversionHelpers()

  /** Indices and hints. */
  val scoreIdIndex = MongoDBObject("score" -> -1, "_id" -> 1)
  itemRecScoreColl.ensureIndex(scoreIdIndex)
  //itemRecScoreColl.ensureIndex(MongoDBObject("algoid" -> 1, "uid" -> 1, "modelset" -> 1))
  itemRecScoreColl.ensureIndex(MongoDBObject("algoid" -> 1, "uid" -> 1))

  def getTopN(uid: String, n: Int, itypes: Option[Seq[String]], after: Option[RealTimeItemRecScore])(implicit app: App, algo: Algo, offlineEval: Option[OfflineEval] = None) = {
    //val modelset = offlineEval map { _ => false } getOrElse algo.modelset
    val query = MongoDBObject("algoid" -> algo.id, "uid" -> idWithAppid(app.id, uid)) ++
      (itypes map { loi => MongoDBObject("itypes" -> MongoDBObject("$in" -> loi)) } getOrElse emptyObj)
    after map { irs =>
      new MongoRealTimeItemRecScoreIterator(
        itemRecScoreColl.find(query).
          $min(MongoDBObject("score" -> irs.score, "_id" -> irs.id)).
          sort(scoreIdIndex).
          skip(1).limit(n),
        app.id
      )
    } getOrElse new MongoRealTimeItemRecScoreIterator(
      itemRecScoreColl.find(query).sort(scoreIdIndex).limit(n),
      app.id
    )
  }

  def save(itemrecscore: RealTimeItemRecScore) = {
    val uid = idWithAppid(itemrecscore.appid, itemrecscore.uid)
    val iid = idWithAppid(itemrecscore.appid, itemrecscore.iid)
    // val id = new ObjectId
    // use uid_iid as mongo _id so guarantee each uid iid pair is unique
    val id = s"${itemrecscore.algoid}_${uid}_${iid}" //new ObjectId
    val itemRecObj = MongoDBObject(
      "_id" -> id,
      "uid" -> uid,
      "iid" -> iid,
      "score" -> itemrecscore.score,
      "time" -> itemrecscore.time,
      "itypes" -> itemrecscore.itypes,
      "algoid" -> itemrecscore.algoid
    //"modelset" -> itemrecscore.modelset
    )
    itemRecScoreColl.save(itemRecObj)
    itemrecscore.copy(id = Some(id))
  }

  def deleteByAlgoid(algoid: Int) = {
    itemRecScoreColl.remove(MongoDBObject("algoid" -> algoid))
  }

  /*def deleteByAlgoidAndModelset(algoid: Int, modelset: Boolean) = {
    itemRecScoreColl.remove(MongoDBObject("algoid" -> algoid, "modelset" -> modelset))
  }*/

  def deleteOldByAlgoidAndTime(algoid: Int, time: DateTime) = {
    itemRecScoreColl.remove(MongoDBObject("algoid" -> algoid, "time" -> MongoDBObject("$lte" -> time)))
  }

  def existByAlgo(algo: Algo) = {
    itemRecScoreColl.findOne(MongoDBObject("algoid" -> algo.id)) map { _ => true } getOrElse false
  }

  /** Private mapping function to map DB Object to ItemRecScore object */
  private def dbObjToRealTimeItemRecScore(dbObj: DBObject, appid: Int) = {
    RealTimeItemRecScore(
      uid = dbObj.as[String]("uid").drop(appid.toString.length + 1),
      iid = dbObj.as[String]("iid").drop(appid.toString.length + 1),
      score = dbObj.as[Double]("score"),
      time = dbObj.as[DateTime]("time"),
      itypes = mongoDbListToListOfString(dbObj.as[MongoDBList]("itypes")),
      appid = appid,
      algoid = dbObj.as[Int]("algoid"),
      //modelset = dbObj.as[Boolean]("modelset"),
      id = Some(dbObj.as[String]("_id"))
    )
  }

  class MongoRealTimeItemRecScoreIterator(it: MongoCursor, appid: Int) extends Iterator[RealTimeItemRecScore] {
    def hasNext = it.hasNext
    def next = dbObjToRealTimeItemRecScore(it.next, appid)
  }
}
