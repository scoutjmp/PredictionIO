package io.prediction.commons.modeldata

import io.prediction.commons.settings.{ Algo, App }

import org.specs2._
import org.specs2.specification.Step
import com.mongodb.casbah.Imports._
import com.github.nscala_time.time.Imports._

class RealTimeItemRecScoresSpec extends Specification {
  def is =
    "PredictionIO Model Data Real Time Item Recommendation Scores Specification" ^
      p ^
      "ItemRecScores can be implemented by:" ^ endp ^
      "1. MongoRealTimeItemRecScores" ^ mongoRealTimeItemRecScores ^ end

  def mongoRealTimeItemRecScores = p ^
    "MongoRealTimeItemRecScores should" ^
    "behave like any RealTimeItemRecScores implementation" ^ itemRecScores(newMongoRealTimeItemRecScores) ^
    Step(MongoConnection()(mongoDbName).dropDatabase())

  def itemRecScores(itemRecScores: RealTimeItemRecScores) = {
    t ^
      "saving and getting 3 RealTimeItemRecScores" ! save(itemRecScores) ^
      "getting 4+4+2 RealTimeItemRecScores" ! getTopN(itemRecScores) ^
      "delete RealTimeItemRecScores by algoid" ! deleteByAlgoid(itemRecScores) ^
      "delete old RealTimeItemRecScores by algoid and time" ! deleteOldByAlgoidAndTime(itemRecScores) ^
      "existence by Algo" ! existByAlgo(itemRecScores) ^
      bt
  }

  val mongoDbName = "predictionio_modeldata_mongorealtimeitemrecscore_test"

  def newMongoRealTimeItemRecScores = new mongodb.MongoRealTimeItemRecScores(MongoConnection()(mongoDbName))

  def save(itemRecScores: RealTimeItemRecScores) = {
    implicit val app = App(
      id = 0,
      userid = 0,
      appkey = "",
      display = "",
      url = None,
      cat = None,
      desc = None,
      timezone = "UTC"
    )
    implicit val algo = Algo(
      id = 1,
      engineid = 0,
      name = "",
      infoid = "abc",
      command = "",
      params = Map(),
      settings = Map(),
      modelset = true,
      createtime = DateTime.now,
      updatetime = DateTime.now,
      status = "deployed",
      offlineevalid = None,
      offlinetuneid = None,
      loop = None,
      paramset = None
    )
    val itemScores = List(RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem1",
      score = -5.6,
      time = DateTime.now.hour(23).minute(13),
      itypes = List("1", "2", "3"),
      appid = app.id,
      algoid = algo.id
    //      modelset = true
    ), RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem2",
      score = 10,
      time = DateTime.now.hour(23).minute(14),
      itypes = List("4", "5", "6"),
      appid = app.id,
      algoid = algo.id
    //      modelset = true
    ), RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem3",
      score = 124.678,
      time = DateTime.now.hour(23).minute(15),
      itypes = List("7", "8", "9"),
      appid = app.id,
      algoid = algo.id
    //      modelset = true
    ), RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem4",
      score = 999,
      time = DateTime.now.hour(23).minute(16),
      itypes = List("invalid"),
      appid = app.id,
      algoid = algo.id
    //modelset = true
    ))
    val dbItemScores = itemScores map {
      itemRecScores.save(_)
    }
    val results = itemRecScores.getTopN("testUser", 4, Some(List("1", "5", "9")), None)
    val r1 = results.next
    val r2 = results.next
    val r3 = results.next()
    results.hasNext must beFalse and
      (r1 must beEqualTo(dbItemScores(2))) and
      (r2 must beEqualTo(dbItemScores(1))) and
      (r3 must beEqualTo(dbItemScores(0)))
  }

  def getTopN(itemRecScores: RealTimeItemRecScores) = {
    implicit val app = App(
      id = 234,
      userid = 0,
      appkey = "",
      display = "",
      url = None,
      cat = None,
      desc = None,
      timezone = "UTC"
    )
    implicit val algo = Algo(
      id = 234,
      engineid = 0,
      name = "",
      infoid = "abc",
      command = "",
      params = Map(),
      settings = Map(),
      modelset = true,
      createtime = DateTime.now,
      updatetime = DateTime.now,
      status = "deployed",
      offlineevalid = None,
      offlinetuneid = None,
      loop = None,
      paramset = None
    )
    val itemScores = List(RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem1",
      score = -5.6,
      time = DateTime.now.hour(23).minute(16),
      itypes = List("1", "2", "3"),
      appid = app.id,
      algoid = algo.id
    //modelset = true
    ), RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem2",
      score = 10,
      time = DateTime.now.hour(23).minute(17),
      itypes = List("4", "5", "6"),
      appid = app.id,
      algoid = algo.id
    //modelset = true
    ), RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem3",
      score = 124.678,
      time = DateTime.now.hour(23).minute(18),
      itypes = List("7", "8", "9"),
      appid = app.id,
      algoid = algo.id
    //modelset = true
    ), RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem4",
      score = 9995,
      time = DateTime.now.hour(23).minute(19),
      itypes = List("invalid"),
      appid = app.id,
      algoid = algo.id
    //modelset = true
    ), RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem5",
      score = -5.6,
      time = DateTime.now.hour(23).minute(20),
      itypes = List("1", "2", "3"),
      appid = app.id,
      algoid = algo.id
    //modelset = true
    ), RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem6",
      score = 10,
      time = DateTime.now.hour(23).minute(21),
      itypes = List("4", "5", "6"),
      appid = app.id,
      algoid = algo.id
    //modelset = true
    ), RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem7",
      score = 124.678,
      time = DateTime.now.hour(23).minute(22),
      itypes = List("7", "8", "9"),
      appid = app.id,
      algoid = algo.id
    //modelset = true
    ), RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem8",
      score = 9994,
      time = DateTime.now.hour(23).minute(23),
      itypes = List("invalid"),
      appid = app.id,
      algoid = algo.id
    //modelset = true
    ), RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem9",
      score = 124.678,
      time = DateTime.now.hour(23).minute(24),
      itypes = List("7", "8", "9"),
      appid = app.id,
      algoid = algo.id
    //modelset = true
    ), RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem10",
      score = 9993,
      time = DateTime.now.hour(23).minute(25),
      itypes = List("invalid"),
      appid = app.id,
      algoid = algo.id
    //modelset = true
    ))
    val dbItemScores = itemScores map {
      itemRecScores.save(_)
    }
    val results1234 = itemRecScores.getTopN("testUser", 4, Some(List("invalid", "8", "7", "6", "4", "3", "2", "1", "5", "9")), None)
    val r1 = results1234.next
    val r2 = results1234.next
    val r3 = results1234.next
    val r4 = results1234.next
    val results5678 = itemRecScores.getTopN("testUser", 4, Some(List("invalid", "8", "7", "6", "4", "3", "2", "1", "5", "9")), Some(r4))
    val r5 = results5678.next
    val r6 = results5678.next
    val r7 = results5678.next
    val r8 = results5678.next
    val results910 = itemRecScores.getTopN("testUser", 4, Some(List("invalid", "8", "7", "6", "4", "3", "2", "1", "5", "9")), Some(r8))
    val r9 = results910.next
    val r10 = results910.next
    results1234.hasNext must beFalse and
      (r1 must beEqualTo(dbItemScores(3))) and
      (r2 must beEqualTo(dbItemScores(7))) and
      (r3 must beEqualTo(dbItemScores(9))) and
      (r4 must beEqualTo(dbItemScores(2))) and
      (results5678.hasNext must beFalse) and
      (r5 must beEqualTo(dbItemScores(6))) and
      (r6 must beEqualTo(dbItemScores(8))) and
      (r7 must beEqualTo(dbItemScores(1))) and
      (r8 must beEqualTo(dbItemScores(5))) and
      (results910.hasNext must beFalse) and
      (r9 must beEqualTo(dbItemScores(0))) and
      (r10 must beEqualTo(dbItemScores(4)))
  }

  def deleteByAlgoid(itemRecScores: RealTimeItemRecScores) = {

    implicit val app = App(
      id = 0,
      userid = 0,
      appkey = "",
      display = "",
      url = None,
      cat = None,
      desc = None,
      timezone = "UTC"
    )

    val algo1 = Algo(
      id = 19999,
      engineid = 0,
      name = "algo1",
      infoid = "abc",
      command = "",
      params = Map(),
      settings = Map(),
      modelset = true,
      createtime = DateTime.now,
      updatetime = DateTime.now,
      status = "deployed",
      offlineevalid = None,
      offlinetuneid = None,
      loop = None,
      paramset = None
    )

    val algo2 = algo1.copy(id = 2) // NOTE: different id

    val itemScores1 = List(RealTimeItemRecScore(
      uid = "deleteByAlgoidUser",
      iid = "testUserItem1",
      score = -5.6,
      time = DateTime.now.hour(23).minute(25),
      itypes = List("1", "2", "3"),
      appid = app.id,
      algoid = algo1.id
    //modelset = algo1.modelset
    ), RealTimeItemRecScore(
      uid = "deleteByAlgoidUser",
      iid = "testUserItem2",
      score = 10,
      time = DateTime.now.hour(23).minute(26),
      itypes = List("4", "5", "6"),
      appid = app.id,
      algoid = algo1.id
    //modelset = algo1.modelset
    ), RealTimeItemRecScore(
      uid = "deleteByAlgoidUser",
      iid = "testUserItem3",
      score = 124.678,
      time = DateTime.now.hour(23).minute(27),
      itypes = List("7", "8", "9"),
      appid = app.id,
      algoid = algo1.id
    //modelset = algo1.modelset
    ), RealTimeItemRecScore(
      uid = "deleteByAlgoidUser",
      iid = "testUserItem4",
      score = 999,
      time = DateTime.now.hour(23).minute(28),
      itypes = List("invalid"),
      appid = app.id,
      algoid = algo1.id
    //modelset = algo1.modelset
    ))

    val itemScores2 = List(RealTimeItemRecScore(
      uid = "deleteByAlgoidUser",
      iid = "testUserItem1",
      score = 3,
      time = DateTime.now.hour(23).minute(29),
      itypes = List("1", "2", "3"),
      appid = app.id,
      algoid = algo2.id
    //modelset = algo2.modelset
    ), RealTimeItemRecScore(
      uid = "deleteByAlgoidUser",
      iid = "testUserItem2",
      score = 2,
      time = DateTime.now.hour(23).minute(30),
      itypes = List("4", "5", "6"),
      appid = app.id,
      algoid = algo2.id
    //modelset = algo2.modelset
    ), RealTimeItemRecScore(
      uid = "deleteByAlgoidUser",
      iid = "testUserItem3",
      score = 1,
      time = DateTime.now.hour(23).minute(31),
      itypes = List("7", "8", "9"),
      appid = app.id,
      algoid = algo2.id
    //modelset = algo2.modelset
    ), RealTimeItemRecScore(
      uid = "deleteByAlgoidUser",
      iid = "testUserItem4",
      score = 0,
      time = DateTime.now.hour(23).minute(32),
      itypes = List("invalid"),
      appid = app.id,
      algoid = algo2.id
    //modelset = algo2.modelset
    ))

    val dbItemScores1 = itemScores1 map {
      itemRecScores.save(_)
    }

    val dbItemScores2 = itemScores2 map {
      itemRecScores.save(_)
    }

    val results1 = itemRecScores.getTopN("deleteByAlgoidUser", 4, None, None)(app, algo1)
    val r1r1 = results1.next
    val r1r2 = results1.next
    val r1r3 = results1.next
    val r1r4 = results1.next

    val results2 = itemRecScores.getTopN("deleteByAlgoidUser", 4, None, None)(app, algo2)
    val r2r1 = results2.next
    val r2r2 = results2.next
    val r2r3 = results2.next
    val r2r4 = results2.next

    itemRecScores.deleteByAlgoid(algo1.id)

    val results1b = itemRecScores.getTopN("deleteByAlgoidUser", 4, None, None)(app, algo1)

    val results2b = itemRecScores.getTopN("deleteByAlgoidUser", 4, None, None)(app, algo2)
    val r2br1 = results2b.next
    val r2br2 = results2b.next
    val r2br3 = results2b.next
    val r2br4 = results2b.next

    itemRecScores.deleteByAlgoid(algo2.id)
    val results2c = itemRecScores.getTopN("deleteByAlgoidUser", 4, None, None)(app, algo2)

    results1.hasNext must beFalse and
      (r1r1 must beEqualTo(dbItemScores1(3))) and
      (r1r2 must beEqualTo(dbItemScores1(2))) and
      (r1r3 must beEqualTo(dbItemScores1(1))) and
      (r1r4 must beEqualTo(dbItemScores1(0))) and
      (results2.hasNext must beFalse) and
      (r2r1 must beEqualTo(dbItemScores2(0))) and
      (r2r2 must beEqualTo(dbItemScores2(1))) and
      (r2r3 must beEqualTo(dbItemScores2(2))) and
      (r2r4 must beEqualTo(dbItemScores2(3))) and
      (results1b.hasNext must beFalse) and
      (results2b.hasNext must beFalse) and
      (r2br1 must beEqualTo(dbItemScores2(0))) and
      (r2br2 must beEqualTo(dbItemScores2(1))) and
      (r2br3 must beEqualTo(dbItemScores2(2))) and
      (r2br4 must beEqualTo(dbItemScores2(3))) and
      (results2c.hasNext must beFalse)
  }

  def deleteOldByAlgoidAndTime(itemRecScores: RealTimeItemRecScores) = {

    implicit val app = App(
      id = 0,
      userid = 0,
      appkey = "",
      display = "",
      url = None,
      cat = None,
      desc = None,
      timezone = "UTC"
    )

    val algo1 = Algo(
      id = 13333,
      engineid = 0,
      name = "algo1",
      infoid = "abc",
      command = "",
      params = Map(),
      settings = Map(),
      modelset = true,
      createtime = DateTime.now,
      updatetime = DateTime.now,
      status = "deployed",
      offlineevalid = None,
      offlinetuneid = None,
      loop = None,
      paramset = None
    )

    val algo2 = algo1.copy(id = 13334) // NOTE: different id

    val itemScores1 = List(RealTimeItemRecScore(
      uid = "deleteOldByAlgoidAndTimeUser",
      iid = "testUserItem1",
      score = -5.6,
      time = new DateTime(2013, 11, 3, 10, 11, 12),
      itypes = List("1", "2", "3"),
      appid = app.id,
      algoid = algo1.id
    //modelset = algo1.modelset
    ), RealTimeItemRecScore(
      uid = "deleteOldByAlgoidAndTimeUser",
      iid = "testUserItem2",
      score = 10,
      time = new DateTime(2013, 11, 3, 10, 11, 13),
      itypes = List("4", "5", "6"),
      appid = app.id,
      algoid = algo1.id
    //modelset = algo1.modelset
    ), RealTimeItemRecScore(
      uid = "deleteOldByAlgoidAndTimeUser",
      iid = "testUserItem3",
      score = 124.678,
      time = new DateTime(2013, 11, 3, 10, 11, 14),
      itypes = List("7", "8", "9"),
      appid = app.id,
      algoid = algo1.id
    //modelset = algo1.modelset
    ), RealTimeItemRecScore(
      uid = "deleteOldByAlgoidAndTimeUser",
      iid = "testUserItem4",
      score = 999,
      time = new DateTime(2013, 11, 3, 10, 11, 15),
      itypes = List("invalid"),
      appid = app.id,
      algoid = algo1.id
    //modelset = algo1.modelset
    ))

    val itemScores2 = List(RealTimeItemRecScore(
      uid = "deleteOldByAlgoidAndTimeUser",
      iid = "testUserItem1",
      score = 3,
      time = new DateTime(2013, 11, 3, 10, 11, 15),
      itypes = List("1", "2", "3"),
      appid = app.id,
      algoid = algo2.id
    //modelset = algo2.modelset
    ), RealTimeItemRecScore(
      uid = "deleteOldByAlgoidAndTimeUser",
      iid = "testUserItem2",
      score = 2,
      time = new DateTime(2013, 11, 3, 10, 11, 14),
      itypes = List("4", "5", "6"),
      appid = app.id,
      algoid = algo2.id
    //modelset = algo2.modelset
    ), RealTimeItemRecScore(
      uid = "deleteOldByAlgoidAndTimeUser",
      iid = "testUserItem3",
      score = 1,
      time = new DateTime(2013, 11, 3, 10, 11, 13),
      itypes = List("7", "8", "9"),
      appid = app.id,
      algoid = algo2.id
    //modelset = algo2.modelset
    ), RealTimeItemRecScore(
      uid = "deleteOldByAlgoidAndTimeUser",
      iid = "testUserItem4",
      score = 0,
      time = new DateTime(2013, 11, 3, 10, 11, 12),
      itypes = List("invalid"),
      appid = app.id,
      algoid = algo2.id
    //modelset = algo2.modelset
    ))

    val dbItemScores1 = itemScores1 map {
      itemRecScores.save(_)
    }

    val dbItemScores2 = itemScores2 map {
      itemRecScores.save(_)
    }

    val results1 = itemRecScores.getTopN("deleteOldByAlgoidAndTimeUser", 4, None, None)(app, algo1)
    val r1r1 = results1.next
    val r1r2 = results1.next
    val r1r3 = results1.next
    val r1r4 = results1.next

    val results2 = itemRecScores.getTopN("deleteOldByAlgoidAndTimeUser", 4, None, None)(app, algo2)
    val r2r1 = results2.next
    val r2r2 = results2.next
    val r2r3 = results2.next
    val r2r4 = results2.next

    itemRecScores.deleteOldByAlgoidAndTime(algo1.id, new DateTime(2013, 11, 3, 10, 11, 13))
    itemRecScores.deleteOldByAlgoidAndTime(algo2.id, new DateTime(2013, 11, 3, 10, 11, 12))

    val results1b = itemRecScores.getTopN("deleteOldByAlgoidAndTimeUser", 4, None, None)(app, algo1)
    val r1br1 = results1b.next
    val r1br2 = results1b.next

    val results2b = itemRecScores.getTopN("deleteOldByAlgoidAndTimeUser", 4, None, None)(app, algo2)
    val r2br1 = results2b.next
    val r2br2 = results2b.next
    val r2br3 = results2b.next

    itemRecScores.deleteOldByAlgoidAndTime(algo1.id, new DateTime(2013, 11, 3, 10, 12, 12))
    itemRecScores.deleteOldByAlgoidAndTime(algo2.id, new DateTime(2013, 11, 3, 10, 12, 12))

    val results1c = itemRecScores.getTopN("deleteOldByAlgoidAndTimeUser", 4, None, None)(app, algo1)
    val results2c = itemRecScores.getTopN("deleteOldByAlgoidAndTimeUser", 4, None, None)(app, algo2)

    results1.hasNext must beFalse and
      (r1r1 must beEqualTo(dbItemScores1(3))) and
      (r1r2 must beEqualTo(dbItemScores1(2))) and
      (r1r3 must beEqualTo(dbItemScores1(1))) and
      (r1r4 must beEqualTo(dbItemScores1(0))) and
      (results2.hasNext must beFalse) and
      (r2r1 must beEqualTo(dbItemScores2(0))) and
      (r2r2 must beEqualTo(dbItemScores2(1))) and
      (r2r3 must beEqualTo(dbItemScores2(2))) and
      (r2r4 must beEqualTo(dbItemScores2(3))) and
      (results1b.hasNext must beFalse) and
      (r1br1 must beEqualTo(dbItemScores1(3))) and
      (r1br2 must beEqualTo(dbItemScores1(2))) and
      (results2b.hasNext must beFalse) and
      (r2br1 must beEqualTo(dbItemScores2(0))) and
      (r2br2 must beEqualTo(dbItemScores2(1))) and
      (r2br3 must beEqualTo(dbItemScores2(2))) and
      (results1c.hasNext must beFalse) and
      (results2c.hasNext must beFalse)
  }

  def existByAlgo(itemRecScores: RealTimeItemRecScores) = {
    implicit val app = App(
      id = 345,
      userid = 0,
      appkey = "",
      display = "",
      url = None,
      cat = None,
      desc = None,
      timezone = "UTC"
    )
    val algo1 = Algo(
      id = 345,
      engineid = 0,
      name = "",
      infoid = "dummy",
      command = "",
      params = Map(),
      settings = Map(),
      modelset = true,
      createtime = DateTime.now,
      updatetime = DateTime.now,
      status = "deployed",
      offlineevalid = None,
      offlinetuneid = None,
      loop = None,
      paramset = None
    )
    val algo2 = algo1.copy(id = 3456)
    itemRecScores.save(RealTimeItemRecScore(
      uid = "testUser",
      iid = "testUserItem4",
      score = 999,
      time = DateTime.now.hour(23).minute(33),
      itypes = List("invalid"),
      appid = app.id,
      algoid = algo1.id
    //modelset = algo1.modelset
    ))
    itemRecScores.existByAlgo(algo1) must beTrue and
      (itemRecScores.existByAlgo(algo2) must beFalse)
  }
}
