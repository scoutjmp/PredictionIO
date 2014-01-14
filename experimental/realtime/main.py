
# To Run:
# python -m PredictionIO.experimental.realtime.main

import operator

from PredictionIO.experimental.realtime import appdata
from PredictionIO.experimental.realtime import batch

def Run():
  app_data = appdata.AppData()
  
  rate_actions = app_data._rate_actions
  rate_actions.sort(key=operator.attrgetter('epoch_t'))

  users = app_data._users
  
  items = app_data._items

  item_list = items.keys()

  for i in item_list:
    print items[i]

  item_attributes_map = dict()
  for i in item_list:
    item_attributes_map[i] = items[i].genres


  for k, v in item_attributes_map.iteritems():
    print k, ' -> ', v

  # user -> [items]
  user_actions_map = dict()
  for rate_action in rate_actions:
    uid = rate_action.uid
    iid = rate_action.iid
    actions = user_actions_map.setdefault(uid, [])
    actions.append(iid)
    

  user_df = batch.BatchProcess(users, item_attributes_map, user_actions_map)

  batch.RecommendUserList(item, item_attributes, user_df)


  



if __name__ == '__main__':
  Run()


