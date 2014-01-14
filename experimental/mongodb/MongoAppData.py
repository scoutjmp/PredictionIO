import pymongo
from pymongo import MongoClient

## see http://api.mongodb.org/python/current/tutorial.html for mongo

class MongoItems(object):
    def __init__(self, db_name, db_host, db_port):
        self.client = MongoClient(db_host, db_port)
        self.appdata_db = self.client[db_name]
        self.items_coll = self.appdata_db['items']
        
    def get_by_appid(self, appid, itypes=None):
        """Get items by appid and itypes

        :param appid: app id. type int.
        :param itypes: itypes. type List. eg. ['t1', 't2'] or None

        :returns:
            mongo cursor instance
        """
        if itypes:
            return self.items_coll.find({'appid': appid, 'itypes': { '$in': itypes }})
        else:
            return self.items_coll.find({'appid': appid})
            
    def get_recent_by_appid(self, appid, starttime, itypes=None):
        """Get recent items newer than the specified starttime

        :param appid: app id. type int.
        :param itypes: itypes. type List. eg. ['t1', 't2'] or None
        :param starttime: start time. type datetime.datetime

        :returns:
            mongo cursor instance
        """
        if itypes:
            return self.items_coll.find({'appid': appid, 'types': { '$in': itypes }, 'starttime' : { '$gt' : starttime }})
        else:
            return self.items_coll.find({'appid': appid, 'starttime' : { '$gt' : starttime }})            

class MongoU2IActions(object):
    def __init__(self, db_name, db_host, db_port):
        self.client = MongoClient(db_host, db_port)
        self.appdata_db = self.client[db_name]
        self.u2iactions_coll = self.appdata_db['u2iActions']
        
    def get_by_appid(self, appid):
        """
        :param appid: app id. type int.
        
        :returns:
            mongo cursor instance
        """
        return self.u2iactions_coll.find({'appid': appid})

