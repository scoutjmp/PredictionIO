

from mongodb.MongoAppData import MongoItems
from mongodb.MongoAppData import MongoU2IActions
import datetime
import csv
import argparse
import time

from commons import is_custom_attributes
from commons import ms_to_datetime
from commons import datetime_to_ms


def write_items_to_file(cursor, items_filename, itemsitypes_filename):
    """write items mongo cursor to a file 
    items_filename:
    iid<\t>starttime<\t>attribute separated by tab (arbitrary number of attributes)
    attribute is represent as 'name=value'

    itemsitypes_filename:
    iid<\t>itypes separated by tab (arbitrary number of itypes)

    :params cursor: mongo cursor
    :params items_filename: file name to be written
    """
    with open(items_filename, 'wb') as f, open(itemsitypes_filename, 'wb') as itemsitypes_f:
        writer = csv.writer(f, delimiter='\t')
        itemsitypes_writer = csv.writer(itemsitypes_f, delimiter='\t')

        for item in cursor:
            custom_attr = dict((k,v) for k,v in item.iteritems() if is_custom_attributes(k))

            attr_values = []
            for k,v in custom_attr.iteritems():
                t = "%s=%s" % (k,v)
                attr_values.append(t)

            row = []
            #st = item['starttime'].isoformat()
            st = datetime_to_ms(item['starttime'])
            row.extend([item['_id'], st])
            row.extend(attr_values)
            #print row
            writer.writerow(row)

            itypes_row = [item['_id']] + item['itypes']
            itemsitypes_writer.writerow(itypes_row)

def realtime_dataprep(db_name, db_host, db_port, appid, itypes, starttime, implicit, items_filename, itemsitypes_filename):
    """ read items from DB and write to file
    """
    mongo_items = MongoItems(db_name, db_host, db_port)
    # get recent new items
    items = mongo_items.get_recent_by_appid(appid=appid, starttime=starttime, itypes=itypes)
    write_items_to_file(items, items_filename, itemsitypes_filename)

def main():
    parser = argparse.ArgumentParser(description="some description here..")
    parser.add_argument('--db_name', default='predictionio_appdata')
    parser.add_argument('--db_host', default='localhost')
    parser.add_argument('--db_port', default=27017, type=int)
    parser.add_argument('--appid', type=int) # note must be integer
    parser.add_argument('--itypes')
    parser.add_argument('--starttime', type=int) # in milliseconds
    parser.add_argument('--implicit', default=False) # means no explict rating
    parser.add_argument('--output_items', default='test_realtime_items.tsv') # output items file name
    parser.add_argument('--output_itemsitypes', default='test_realtime_itemsitypes.tsv') # output items file with itypes

    args = parser.parse_args()
    print args

    realtime_dataprep(
        db_name=args.db_name,
        db_host=args.db_host,
        db_port=args.db_port,
        appid=args.appid,
        itypes=args.itypes,
        starttime=ms_to_datetime(args.starttime), # convert starttime from milliseconds to datetime
        implicit=args.implicit,
        items_filename=args.output_items,
        itemsitypes_filename=args.output_itemsitypes)

if __name__ == '__main__':
    main()

