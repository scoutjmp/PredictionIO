import datetime
import time

# ca_ is special prefix reserved for custom attributes
CA_PREFIX = 'ca_'
RESERVED_ATTRIBUTES = ['ca_description'] 
CA_LEN = len(CA_PREFIX)

def is_custom_attributes(name):
    """check if this is custom attribute"""
    r = name.startswith(CA_PREFIX) and name not in RESERVED_ATTRIBUTES
    return r

def ms_to_datetime(ms):
    """
    Convert ms in integer to datetime.datetime
    """
    # fromtimestamp() takes seconds
    dt = datetime.datetime.fromtimestamp(ms/1000).replace(microsecond=ms%1000*1000)
    return dt

def datetime_to_ms(dt):
    return int(time.mktime(dt.timetuple()) * 1000 + dt.microsecond / 1000)
