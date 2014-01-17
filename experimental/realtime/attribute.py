import re
import collections
import logging

# Match consecutive words begins with a captial letter.
two_captial_words_pattern = re.compile(r'\W([A-Z]\w+(?:\s+[A-Z]\w+)+)\W')


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

