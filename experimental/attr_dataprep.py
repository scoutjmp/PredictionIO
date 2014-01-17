

from mongodb.MongoAppData import MongoItems
from mongodb.MongoAppData import MongoU2IActions
import datetime
import csv
import argparse
import time

from commons import is_custom_attributes
from commons import ms_to_datetime
from commons import datetime_to_ms


def write_items_to_file(cursor, items_filename):
    """write items mongo cursor to a file 
    items_filename:
    iid<\t>description

    :params cursor: mongo cursor
    :params items_filename: file name to be written
    """
    with open(items_filename, 'wb') as f:
        writer = csv.writer(f, delimiter='\t')

        for item in cursor:
            if 'ca_description' in item:
                row = [item['_id'], item['ca_description']]
                writer.writerow(row)


def attr_dataprep(db_name, db_host, db_port, appid, itypes, items_filename):
    """ read items from DB and write to file
    """
    mongo_items = MongoItems(db_name, db_host, db_port)
    # get recent new items
    items = mongo_items.get_by_appid(appid=appid, itypes=itypes)
    write_items_to_file(items, items_filename)

def main():
    parser = argparse.ArgumentParser(description="some description here..")
    parser.add_argument('--db_name', default='predictionio_appdata')
    parser.add_argument('--db_host', default='localhost')
    parser.add_argument('--db_port', default=27017, type=int)
    parser.add_argument('--appid', type=int) # note must be integer
    parser.add_argument('--itypes')
    parser.add_argument('--output_items', default='test_item_description.tsv') # output items file name

    args = parser.parse_args()
    print args

    attr_dataprep(
        db_name=args.db_name,
        db_host=args.db_host,
        db_port=args.db_port,
        appid=args.appid,
        itypes=args.itypes,
        items_filename=args.output_items)

if __name__ == '__main__':
    main()

