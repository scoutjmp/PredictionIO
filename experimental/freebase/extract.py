import re
import logging

from PredictionIO.experimental.realtime import appdata
from PredictionIO.experimental.freebase import fb_util

#output_file = 'item.data'
output_dir = '/home/yipjustin/p7/PredictionIO/experimental/freebase/data'

logging.basicConfig(level=logging.INFO) 

def ExtractMid():
  logger = logging.getLogger('extract')

  app_data = appdata.AppData()
  items = app_data._items

  item_list = items.keys()

  no_mid_count = 0
  no_description_count = 0
  
  for i, iid in enumerate(item_list):
    #if i < 23: continue
    logger.info('Progress: %d', i)

    item = items[iid]
    name = item.name
    logger.info('iid: %s, Name: %s', iid, name)
  
    # Need to remove year from movie name
    normalized_name = re.sub(r'\(\d+\)', '', name) 
    keyword_result = fb_util.KeywordSearch(normalized_name)

    if keyword_result:
      mid, fb_name = keyword_result

      logger.info('Freebase mid: %s name: %s', mid, fb_name)

      description = fb_util.GetDescription(mid).replace('\n', ' ')

      if not description:
        no_description_count += 1

      description = description.encode('utf-8')

      logger.info('Description: %s', description)
    else:
      mid = ''
      description = ''
      logger.info('Not Found')

      no_mid_count += 1

    with open(output_dir + '/' + iid + '.txt', 'w') as output_f:
      output_f.write(name + '\n')
      output_f.write(mid + '\n')
      output_f.write(description + '\n')

  logger.info('Total: %d', len(item_list))
  logger.info('No mid: %d', no_mid_count)
  logger.info('No description: %d', no_description_count)


if __name__ == '__main__':
  ExtractMid()
