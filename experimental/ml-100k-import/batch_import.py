
from appdata import AppData
import predictionio
import sys
import pprint

from app_config import APP_KEY, API_URL, THREADS, REQUEST_QSIZE

data_dir = '../freebase/data/'

def batch_import_task(app_data, client, all_info=False):

	print "[Info] Importing users to PredictionIO..."
	count = 0
	for k, v in app_data.get_users().iteritems():
		count += 1
		if all_info:
			print "[Info] Importing %s..." % v
		else:
			if (count % 32 == 0):
				sys.stdout.write('\r[Info] %s' % count)
				sys.stdout.flush()

		client.acreate_user(v.uid)

	sys.stdout.write('\r[Info] %s users were imported.\n' % count)
	sys.stdout.flush()

	item_description_map = dict()
	item_list = app_data._items.keys()
	for iid in item_list:
		fn = '{data_dir}{iid}.txt'.format(data_dir=data_dir, iid=iid)
		with open(fn, 'r') as f:
			name = f.readline().strip()
			mid = f.readline().strip()
			description = f.readline().strip()

		item_description_map[iid] = description

	print "[Info] Importing items to PredictionIO..."
	count = 0
	for k, v in app_data.get_items().iteritems():
		count += 1
		if all_info:
			print "[Info] Importing %s..." % v
		else:
			if (count % 32 == 0):
				sys.stdout.write('\r[Info] %s' % count)
				sys.stdout.flush()

		#itypes = ("movie",) + v.genres
		itypes = ("movie",)
		attr = { "pio_startT" : v.release_date.isoformat() }
		attr.update(dict(zip(v.genres, [1] * len(v.genres))))
		if v.iid in item_description_map:
			attr.update({"description" : item_description_map[v.iid]})

		pprint.pprint(attr)

		client.acreate_item(v.iid, itypes, attr)

	sys.stdout.write('\r[Info] %s items were imported.\n' % count)
	sys.stdout.flush()

	print "[Info] Importing rate actions to PredictionIO..."
	count = 0
	for v in app_data.get_rate_actions():
		count += 1
		if all_info:
			print "[Info] Importing %s..." % v
		else:
			if (count % 32 == 0):
				sys.stdout.write('\r[Info] %s' % count)
				sys.stdout.flush()

		client.identify(v.uid)
		client.arecord_action_on_item("rate", v.iid, { "pio_rate": v.rating, "pio_t": v.t })

	sys.stdout.write('\r[Info] %s rate actions were imported.\n' % count)
	sys.stdout.flush()


if __name__ == '__main__':

	app_data = AppData()
	client = predictionio.Client(APP_KEY, THREADS, API_URL, qsize=REQUEST_QSIZE)
	batch_import_task(app_data, client)
	client.close()

