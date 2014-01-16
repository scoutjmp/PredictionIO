import json
import urllib

api_key = open("/home/yipjustin/p7/PredictionIO/experimental/freebase/google_api_key").read()

def TopicSearch(mid, verbose=False):
  service_url = 'https://www.googleapis.com/freebase/v1/topic'
  params = {
    'key': api_key,
    'filter': 'suggest',
    'indent': True,
  }
  url = service_url + mid + '?' + urllib.urlencode(params)
  response = urllib.urlopen(url).read()
  topic = json.loads(response)

  if verbose: 
    print json.dumps(topic, indent=2, separators=(',', ': '))
  
  return topic


def GetDescription(mid, verbose=False):
  topic = TopicSearch(mid, verbose)
  if 'property' not in topic:
    return ''
  if '/common/topic/article' not in topic['property']:
    return ''
  
  article = topic['property']['/common/topic/article']

  for value in article['values']:
    for sub_property, sub_value in value['property'].iteritems():
      if sub_property != '/common/document/text':
        continue
  
      for sub_sub_value in sub_value['values']:
        if sub_sub_value['lang'] != 'en':
          continue
        return sub_sub_value['value']

  return ''


def KeywordSearch(query):
  service_url = 'https://www.googleapis.com/freebase/v1/search'
  params = {
      'query': query,
      'key': api_key,
      'type': '/film/film',
  }
  url = service_url + '?' + urllib.urlencode(params)
  response = json.loads(urllib.urlopen(url).read())

  if not len(response['result']):
    return None

  result = response['result'][0]

  return result['mid'], result['name']


if __name__ == '__main__':
  #query = 'toy story'
  #KeywordSearch(query)

  mid = '/m/0k95mpr'
  #TopicSearch(mid)
  print GetDescription(mid, verbose=True)
  
