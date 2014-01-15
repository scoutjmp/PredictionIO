# To Run:
# python -m PredictionIO.experimental.realtime.realtime_model \
#   --item=i.tsv --action=r.tsv --output=u.pkl

import argparse
import operator
import logging
import csv
import pickle

from PredictionIO.experimental.realtime import appdata
from PredictionIO.experimental.realtime import batch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('model')

def CreateModel(item_fn, action_fn, output_fn):
  with open(item_fn, 'r') as item_f, open(action_fn, 'r') as action_f:
    item_tsv = csv.reader(item_f, delimiter='\t')
    action_tsv = csv.reader(action_f, delimiter='\t')

    logger.info('Read item from %s', item_fn)
    item_attributes_map = dict()
    for item_row in item_tsv:
      item_attributes_map[item_row[0]] = item_row[2:]

    logger.info('Read action from %s', action_fn)
    user_items_map = dict()
    for action_row in action_tsv:
      uid = action_row[0]
      iid = action_row[1]
      user_items = user_items_map.setdefault(uid, [])
      user_items.append(iid)

  logger.info('Creating user model')
  user_model = batch.BatchProcess(
      users=None,
      item_attributes_map=item_attributes_map,
      user_items_map=user_items_map)

  logger.info('Pickling user model') 
  with open(output_fn, 'wb') as output_f:
    pickle.dump(user_model, output_f)


  return user_model


def Main():
  parser = argparse.ArgumentParser(description='')

  parser.add_argument('--item', default='')
  parser.add_argument('--action', default='')
  parser.add_argument('--output', default='output is a model file')

  args = parser.parse_args()

  user_model = CreateModel(args.item, args.action, args.output)
  print user_model
  

if __name__ == '__main__':
  Main()

