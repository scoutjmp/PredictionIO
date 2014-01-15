# To Run:
#py -m PredictionIO.experimental.realtime.realtime_recommendation \
#  --user_model=u.pkl --new_item=i.tsv --output=recommend.tsv

import argparse
import operator
import logging
import csv
import pickle

from PredictionIO.experimental.realtime import batch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('recommend')

def ExtractRecommendation(user_model_fn, new_item_fn, output_fn):
  logger.info('Load model from %s', user_model_fn)
  with open(user_model_fn, 'rb') as user_model_f:
    user_model = pickle.load(user_model_f)
  
  with open(new_item_fn, 'r') as new_item_f:
    new_item_tsv = csv.reader(new_item_f, delimiter='\t')
    logger.info('Read item from %s', new_item_fn)
    item_attributes_map = dict()
    item_timestamp_map = dict()
    for item_row in new_item_tsv:
      #print item_row
      item_attributes_map[item_row[0]] = item_row[2:]
      item_timestamp_map[item_row[0]] = int(item_row[1])

  output_data = []  # [(user, item, score, timestamp)]

  for item, attributes in item_attributes_map.iteritems():
    user_series = batch.RecommendUserList(item, attributes, user_model)
    for user in user_series.index:
      output_row = (user, item, user_series[user], item_timestamp_map[item])
      output_data.append(output_row)
 
  with open(output_fn, 'w') as output_f:
    tsv_writer = csv.writer(output_f, delimiter='\t')
    tsv_writer.writerows(output_data)

  return output_data
  

def Main():
  parser = argparse.ArgumentParser(description='')

  parser.add_argument('--user_model', default='')
  parser.add_argument('--new_item', default='')
  parser.add_argument('--output', default='')

  args = parser.parse_args()

  recommendation_list = ExtractRecommendation(
      args.user_model, args.new_item, args.output)
  print '\n'.join(map(str,recommendation_list))
  

if __name__ == '__main__':
  Main()


