from mongodb.MongoAppData import MongoRealTimeItemRecScores
from mongodb.MongoAppData import RealTimeItemRecScore

import datetime
import csv
import sys, getopt
import argparse
import time

from commons import ms_to_datetime

def realtime_modelcon(db_name, db_host, db_port, algoid, items_filename, recommend_filename):
    # read items
    # read recommend_filename
    # get itypes for this item
    # save item rec score to mongo

    item_itypes_map = dict()
    with open(items_filename, 'r') as item_f:
        # iid <tab> itypes 
        # itypes are separated by tab (arbitrary number of itypes)
        # eg
        # i1  t1  t2  t3
        # i2  t3
        item_tsv = csv.reader(item_f, delimiter='\t')
        for item_row in item_tsv:
            item_itypes_map[item_row[0]] = item_row[1:]

    with open(recommend_filename, 'r') as rec_f:
        mongo_realtimeitemrec = MongoRealTimeItemRecScores(db_name, db_host, db_port)
        # uid <tab> iid <tab> score <tab> timestamp
        rec_tsv = csv.reader(rec_f, delimiter='\t')
        for rec_row in rec_tsv:
            itemrec = RealTimeItemRecScore(
                uid = rec_row[0],
                iid = rec_row[1],
                score = float(rec_row[2])
                time = ms_to_datetime(rec_row[3]),
                itypes = item_itypes_map[iid],
                algoid = algoid
            )
            mongo_realtimeitemrec.save(itemrec)

def main():
    parser = argparse.ArgumentParser(description="some description here..")
    parser.add_argument('--db_name', default='predictionio_modeldata')
    parser.add_argument('--db_host', default='localhost')
    parser.add_argument('--db_port', default=27017, type=int)
    parser.add_argument('--algoid', type=int) # note must be integer
    parser.add_argument('--input_items', default='test_realtime_itemsitypes.tsv') # items file with itypes
    parser.add_argument('--input_recommendation', default='test_realtime_recommend.tsv')

    realtime_modelcon(
        db_name=args.db_name,
        db_host=args.db_host,
        db_port=args.db_port,
        algoid=args.algoid,
        items_filename=args.input_items,
        recommend_filename=args.input_recommendation
        )

if __name__ == '__main__':
    main()
