# Usage:
# py -m PredictionIO.experimental.realtime.attribute_extraction


import re
import collections
import logging

from PredictionIO.experimental.realtime import appdata

# Match consecutive words begins with a captial letter.
two_captial_words_pattern = re.compile(r'\W([A-Z]\w+(?:\s+[A-Z]\w+)+)\W')


data_dir = 'PredictionIO/experimental/freebase/data/'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('amodel')


def ExtractKeywordsFromText(text):
  return two_captial_words_pattern.findall(text)


def ConstructModel(corpus):
  # corpus is a list of text. 
  # returns a dict of keyword to count
  attribute_count = collections.defaultdict(int)
  for text in corpus:
    matched_list = ExtractKeywordsFromText(text)
    for matched in matched_list:
      attribute_count[matched] += 1
  return attribute_count


def ExtractAttributes(model, text):
  keyword_list = ExtractKeywordsFromText(text)
  
  attribute_list = []
  for keyword in keyword_list:
    if keyword in model:
      attribute_list.append(keyword)

  return list(set(attribute_list))

def Run():
  app_data = appdata.AppData()

  item_list = app_data._items.keys()
  item_count = len(item_list)
  training_count = int(item_count * 0.8)

  training_item_list = item_list[:training_count]
  unseen_item_list = item_list[training_count:]

  item_description_map = dict()

  for iid in item_list:
    fn = '{data_dir}{iid}.txt'.format(data_dir=data_dir, iid=iid)
    with open(fn, 'r') as f:
      name = f.readline().strip()
      mid = f.readline().strip()
      description = f.readline().strip()
    item_description_map[iid] = description

  logger.info('Training')
  description_list = []
  for iid in training_item_list:
    description_list.append(item_description_map[iid])
  model = ConstructModel(description_list)

  attribute_list = [
      (count, attribute) for attribute, count in model.iteritems()]
  attribute_list.sort(reverse=True)

  k = 20
  logger.info('Top Attributes:')
  for a in attribute_list[:k]:
    logger.info('- %s', a)

  for iid in unseen_item_list:
    description = item_description_map[iid]
    attributes = ExtractAttributes(model, description)
    print iid
    print description
    print [(a, model[a]) for a in attributes]
    print
    



if __name__ == '__main__':
  Run()

