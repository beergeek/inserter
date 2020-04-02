try:
    import random
    import string
    import uuid
    import json
    import pymongo
    import random
    import time
    import sys
    from bson import Decimal128
    from datetime import datetime
    import configparser
except ImportError as e:
  print(e)
  sys.exit(1)

config = configparser.ConfigParser()
config.read('mongodb.config')
config_options = {}
try:
    config_options['debug'] = config.getboolean('mongodb','debug',fallback=False)
    config_options['connection_string'] = config.get('mongodb','connection_string')
    config_options['timeout'] = config.get('mongodb','timeout',fallback=1000)
    config_options['db_name'] = config.get('mongodb','db')
    config_options['coll_name'] = config.get('mongodb','collection')
    config_options['batch_size'] = config.getint('mongodb','batch_size',fallback=10)
    config_options['number_docs'] = config.getint('mongodb','no_docs',fallback=10000)
    config_options['sleep'] = config.getint('mongodb','sleep',fallback=10)
    config_options['ssl'] = config.getboolean('mongodb','ssl_enabled',fallback=False)
    if config_options['ssl'] is True:
      config_options['ssl_pem'] = config.get('mongodb','ssl_pem_path',fallback=None)
      config_options['ssl_ca'] = config.get('mongodb', 'ssl_ca_cert_path')
    if config_options['debug'] is True:
        print(config_options)
except (configparser.NoOptionError,configparser.NoSectionError) as e:
    print("""\033[91mERROR! The config file is missing data: %s\033[m""" % e)
    sys.exit(1)

try:
    if config_options['ssl'] is True:
      if config_options['debug'] is True:
        print("Using SSL/TLS to DB")
      if config_options['ssl_pem'] is not None:
        client = pymongo.MongoClient(config_options['connection_string'], serverSelectionTimeoutMS=config_options['timeout'], ssl=True, ssl_certfile=config_options['ssl_pem'], ssl_ca_certs=config_options['ssl_ca'])
      else:
        client = pymongo.MongoClient(config_options['connection_string'], serverSelectionTimeoutMS=config_options['timeout'], ssl=True, ssl_ca_certs=config_options['ssl_ca'])
    else:
      if config_options['debug'] is True:
        print("Not using SSL/TLS to OM DB")
      client = pymongo.MongoClient(config_options['connection_string'], serverSelectionTimeoutMS=config_options['timeout'])
    result = client.admin.command('ismaster')
except pymongo.errors.ServerSelectionTimeoutError as e:
  print("Cannot connect to DB, please check settings in config file: %s" %e)
  raise
except pymongo.errors.ConnectionFailure as e:
  print("Cannot connect to DB, please check settings in config file: %s" %e)
  raise
db = client[config_options['db_name']]
collection = db[config_options['coll_name']]

def getNotes():
  x = []
  y = random.randrange(20)
  for z in range(y):
    x.append("""{"user" : "loudSam",
            "action" : datetime.now()}""")
  return x

def ranString(stringLength = 20):
  lettersAndDigits = string.ascii_letters + string.digits
  return ''.join(random.choice(lettersAndDigits) for i in range(stringLength))

def ranNum(stringLength = 20):
  digits = string.digits
  return ''.join(random.choice(digits) for i in range(stringLength))

def create_json_object():
  x = uuid.uuid4()
  normalCrap = {
    "name": x,
    "a" : ranNum(500),
    "b" : ranString(20),
    "c" : ranString(10),
    "d" : ranString(100),
    "e" : ranNum(33),
    "f" : "TEST",
    "eventEndDtTm" : datetime.now(),
    "eventStartDtTm" : datetime.now(),
    "notes" : getNotes() }
  return normalCrap


collection = db['test_me']
joiner = db['users']
loop_count = 0
for i in range(config_options['number_docs']):
    if loop_count == config_options['batch_size']:
        loop_count = 0
        time.sleep(config_options['sleep'])
    else:
        loop_count += 1
    try:
        first_query = collection.insert_one(create_json_object())
        print(first_query.inserted_id)
    except pymongo.errors.OperationFailure as e:
        print("\033[91mFailure: %s\033[0m" % e)
        sys.exit(1)