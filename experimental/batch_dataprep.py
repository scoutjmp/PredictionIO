

from mongodb.MongoAppData import MongoItems
from mongodb.MongoAppData import MongoU2IActions
import datetime
import csv
import sys, getopt
import argparse


def write_items_to_file(cursor, filename):
    """write items mongo cursor to a file 
    iid<\t>starttime<\t>attribute values separated by tab (arbitrary number of attributes)
    
    :params cursor: mongo cursor
    :params filename: file name to be written
    """
    required_names = ['_id', 'starttime'] # required attribute
    attr_names = [] # keep track all attribute names

    with open(filename, 'wb') as f:
        writer = csv.writer(f, delimiter='\t')
        for item in cursor:
            this_attr = []
            #print item['_id']
            for k in item.iterkeys():
                # ca_ is special prefix reserved for custom attributes
                if k.startswith("ca_"):
                    this_attr.append(k)
                    if k not in attr_names:
                        attr_names.append(k) # attr name kept tarck so far
            #print this_attr
            #print map(lambda a: item[a], this_attr)

            def get_attr_value(name):
                if name in item:
                    return item[name]
                else:
                    return 0

            # attr value of this item in list
            attr_values = map(get_attr_value, attr_names)
            #print attr_values
            row = []
            row.extend([item['_id'], item['starttime'].isoformat()])
            row.extend(attr_values)
            #print row
        
            writer.writerow(row)

    #print required_names + attr_names


def write_u2i_to_file(cursor, filename, implicit=False):
    """process and write u2i mongo cursor to a file
    uid<\t>iid<\t>pref<\t>timestamp in ISO format
    if implicit preference is True:
    uid<\t>iid<\t>timestamp in ISO format

    NOTE: no filtering on same uid-iid action pair yet...

    """
    names = ['uid', 'iid', 'pref', 'time']

    with open(filename, 'wb') as f:
        writer = csv.writer(f, delimiter='\t')
        for u2i in cursor:
            if u2i['action'] != 'rate':
                pref = 1 # use default preference value for implicit action
            else:
                pref = u2i['v']

            if implicit:
                # no preference value
                row = [u2i['uid'], u2i['iid'], u2i['t'].isoformat()]
            else:
                row = [u2i['uid'], u2i['iid'], pref, u2i['t'].isoformat()]
            
            writer.writerow(row)

def batch_dataprep(db_name, db_host, db_port, appid, itypes, implicit, items_filename, pref_filename):
    """ read items from DB and write to file
    """
    mongo_items = MongoItems(db_name, db_host, db_port)
    items = mongo_items.get_by_appid(appid, itypes)
    write_items_to_file(items, items_filename)

    mongo_u2i = MongoU2IActions(db_name, db_host, db_port)
    u2i = mongo_u2i.get_by_appid(appid)
    write_u2i_to_file(u2i, pref_filename, implicit)

def main():
    parser = argparse.ArgumentParser(description="some description here..")
    parser.add_argument('--db_name', default='predictionio_appdata')
    parser.add_argument('--db_host', default='localhost')
    parser.add_argument('--db_port', default=27017)
    parser.add_argument('--appid', type=int) # note must be integer
    parser.add_argument('--itypes')
    parser.add_argument('--implicit', default=False) # means no explict rating
    parser.add_argument('--output_items', default='test_items.tsv') # output items file name
    parser.add_argument('--output_preference', default='test_ratings.tsv') # output preference file name

    args = parser.parse_args()
    print args

    batch_dataprep(
        db_name=args.db_name,
        db_host=args.db_host,
        db_port=args.db_port,
        appid=args.appid,
        itypes=args.itypes,
        implicit=args.implicit,
        items_filename=args.output_items,
        pref_filename=args.output_preference)

if __name__ == '__main__':
    main()

   
