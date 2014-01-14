
from MongoAppData import MongoItems
from MongoAppData import MongoU2IActions
import datetime

APPDATA_DBNAME = 'predictionio_appdata'
APPDATA_DBHOST = 'localhost'
APPDATA_DBPORT = 27017
APP_ID = 17


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


if __name__ == '__main__':
    test_mongo()
    