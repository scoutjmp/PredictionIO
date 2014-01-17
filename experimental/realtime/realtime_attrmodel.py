# To Run:
# python -m PredictionIO.experimental.realtime.realtime_attrmodel \
#   --item=i.tsv --output=a.pkl

import argparse
import operator
import logging
import csv
import pickle
import logging

from PredictionIO.experimental.realtime import attribute

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('amodel')

def CreateAttrModel(item_fn, output_fn):
    with open(item_fn, 'r') as item_f:
        # iid, description
        item_tsv = csv.reader(item_f, delimiter='\t')

        description_list = []
        for item_row in item_tsv:
            description_list.append(item_row[1])

        model = attribute.ConstructModel(description_list)

    with open(output_fn, 'wb') as output_f:
        pickle.dump(model, output_f)

    return model


def Main():
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('--item', default='')
    parser.add_argument('--output', default='output is a model file')

    args = parser.parse_args()

    model = CreateAttrModel(args.item, args.output)
    print model

    attribute_list = [
        (count, attribute) for attribute, count in model.iteritems()]
    attribute_list.sort(reverse=True)

    k = 20
    logger.info('Top Attributes:')
    for a in attribute_list[:k]:
        logger.info('- %s', a)

  
if __name__ == '__main__':
    Main()

