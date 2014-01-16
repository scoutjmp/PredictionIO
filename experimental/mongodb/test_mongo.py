
from MongoAppData import MongoItems
from MongoAppData import MongoU2IActions
from MongoAppData import MongoRealTimeItemRecScores
from MongoAppData import RealTimeItemRecScore
import datetime

APPDATA_DBNAME = 'predictionio_appdata'
APPDATA_DBHOST = 'localhost'
APPDATA_DBPORT = 27017
APP_ID = 17

MODELDATA_DBNAME = "test_predictionio_modeldata"
MODELDATA_DBHOST = 'localhost'
MODELDATA_DBPORT = 27017


def test_mongo():
    mongo_items = MongoItems(APPDATA_DBNAME, APPDATA_DBHOST, APPDATA_DBPORT)

    items = mongo_items.get_by_appid(APP_ID)

    for i in items:
        print i

    print "==="
    items = mongo_items.get_by_appid(APP_ID, ["t2"])
    for i in items:
        print i

    print "==="
    d = datetime.datetime(2014, 1, 11, 15, 12, 39, 536000)
    items = mongo_items.get_recent_by_appid(APP_ID, d)
    for i in items:
        print i

    print "==="
    d = datetime.datetime(2014, 1, 11, 15, 12, 39, 408000)
    items = mongo_items.get_recent_by_appid(APP_ID, d)
    for i in items:
        print i

    print "==="

    mongo_u2i = MongoU2IActions(APPDATA_DBNAME, APPDATA_DBHOST, APPDATA_DBPORT)

    u2i = mongo_u2i.get_by_appid(APP_ID)
    for i in u2i:
        print i

    print "==="

    mongo_realtimeitemrec = MongoRealTimeItemRecScores(MODELDATA_DBNAME, MODELDATA_DBHOST, MODELDATA_DBPORT)

    itemrec1 = RealTimeItemRecScore(
        uid='4',
        iid='5',
        score=2.34,
        time=datetime.datetime(2014, 1, 11, 15, 12, 39, 123),
        itypes=['t1','t2','t3'],
        algoid=6
        )
    itemrec2 = RealTimeItemRecScore(
        uid='4',
        iid='7',
        score=1.34,
        time=datetime.datetime(2014, 1, 11, 15, 12, 39, 124),
        itypes=['t1','t2','t3'],
        algoid=6
        )
    itemrec3 = RealTimeItemRecScore(
        uid='2',
        iid='5',
        score=100.11,
        time=datetime.datetime(2014, 1, 11, 15, 12, 39, 125),
        itypes=['t1','t2','t3'],
        algoid=6
        )

    mongo_realtimeitemrec.save(itemrec1)
    mongo_realtimeitemrec.save(itemrec2)
    mongo_realtimeitemrec.save(itemrec3)

if __name__ == '__main__':
    test_mongo()
    