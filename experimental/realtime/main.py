# To Run:
# python -m PredictionIO.experimental.realtime.main

import operator
import logging

from PredictionIO.experimental.realtime import appdata
from PredictionIO.experimental.realtime import batch

logging.basicConfig(level=logging.INFO)

def Run():
  logger = logging.getLogger('main')

  app_data = appdata.AppData()
  
  rate_actions = app_data._rate_actions
  rate_actions.sort(key=operator.attrgetter('epoch_t'))

  users = app_data._users
  
  items = app_data._items

  item_list = items.keys()

  item_count = len(item_list)

  training_item_count = int(item_count * 0.8)
  logger.info('Training item count: %d', training_item_count)
  logger.info('Unseen item count: %d', item_count - training_item_count)

  training_item_list = item_list[:training_item_count]
  training_item_set = set(training_item_list)
  unseen_item_list = item_list[training_item_count:]


  item_attributes_map = dict()
  for i in training_item_list:
    item_attributes_map[i] = items[i].genres


  # user -> [items]
  user_items_map = dict()
  for rate_action in rate_actions:
    uid = rate_action.uid
    iid = rate_action.iid
    if iid not in training_item_set:
      continue
    actions = user_items_map.setdefault(uid, [])
    actions.append(iid)

  logger.info('Training Model')
  user_df = batch.BatchProcess(users, item_attributes_map, user_items_map)

  logger.info('Prediction')
  for item in unseen_item_list:
    attributes = items[item].genres
    user_list = batch.RecommendUserList(item, attributes, user_df)
    logger.info('item %s, attributes: %s user_list: %s',
        item, attributes, user_list[:5])


if __name__ == '__main__':
  Run()


