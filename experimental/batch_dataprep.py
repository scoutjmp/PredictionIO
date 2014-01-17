

from mongodb.MongoAppData import MongoItems
from mongodb.MongoAppData import MongoU2IActions
import datetime
import csv
import argparse
import time
import pickle

from commons import is_custom_attributes
from commons import ms_to_datetime
from commons import datetime_to_ms
from realtime import attribute

def write_items_to_file(cursor, items_filename, attr_model):
    """write items mongo cursor to a file 
    items_filename:
    iid<\t>starttime<\t>attribute separated by tab (arbitrary number of attributes)
    attribute is represent as 'name=value'

    :params cursor: mongo cursor
    :params items_filename: file name to be written
    :params attr_model attribte model
    """


    with open(items_filename, 'wb') as f:
        writer = csv.writer(f, delimiter='\t')

        for item in cursor:
            custom_attr = dict((k,v) for k,v in item.iteritems() if is_custom_attributes(k))

            attr_values = []
            for k,v in custom_attr.iteritems():
                t = "%s=%s" % (k,v)
                attr_values.append(t)

            ex_attr_values = []
            if 'ca_description' in item:
                extracted_attr = attribute.ExtractAttributes(attr_model, item['ca_description'])
                ex_attr_values = ["ex_%s=%s" % (x.replace(' ','_') ,1) for x in extracted_attr]

            row = []
            #st = item['starttime'].isoformat()
            st = datetime_to_ms(item['starttime'])
            row.extend([item['_id'], st])
            row.extend(attr_values)
            row.extend(ex_attr_values)
            #print row
            writer.writerow(row)

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

            #t = u2i['t'].isoformat()
            t = datetime_to_ms(u2i['t'])

            if implicit:
                # no preference value
                row = [u2i['uid'], u2i['iid'], t]
            else:
                row = [u2i['uid'], u2i['iid'], pref, t]
            
            writer.writerow(row)

def batch_dataprep(db_name, db_host, db_port, appid, itypes, implicit, attr_model_filename, items_filename, pref_filename):
    """ read items from DB and write to file
    """
    
    attr_model = {}
    try:
        with open(attr_model_filename, 'rb') as attr_model_f:
            attr_model = pickle.load(attr_model_f)
    except IOError:
        print "Can't open attribute model %s" % attr_model_filename
        pass # ignore if the attr mode file doesn't exist

    mongo_items = MongoItems(db_name, db_host, db_port)
    items = mongo_items.get_by_appid(appid, itypes)
    write_items_to_file(items, items_filename, attr_model)

    mongo_u2i = MongoU2IActions(db_name, db_host, db_port)
    u2i = mongo_u2i.get_by_appid(appid)
    write_u2i_to_file(u2i, pref_filename, implicit)

def main():
    parser = argparse.ArgumentParser(description="some description here..")
    parser.add_argument('--db_name', default='predictionio_appdata')
    parser.add_argument('--db_host', default='localhost')
    parser.add_argument('--db_port', default=27017, type=int)
    parser.add_argument('--appid', type=int) # note must be integer
    parser.add_argument('--itypes')
    parser.add_argument('--implicit', default=False) # means no explict rating
    parser.add_argument('--input_attr_model', default='test_attr_model.pkl')
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
        attr_model_filename=args.input_attr_model,
        items_filename=args.output_items,
        pref_filename=args.output_preference)

if __name__ == '__main__':
    main()

   
