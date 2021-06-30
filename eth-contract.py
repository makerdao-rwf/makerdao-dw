# NOTE: The first time running this from colab: uncomment the two lines below and run the cell. Then comment them again, restart the runtime, and run this cell again.
#!pip install --force-reinstall jsonschema==3.2.0
#!pip install web3

import json
from io import StringIO
import urllib3
import psycopg2
import pandas as pd
import requests
from web3 import Web3
import asyncio
import time
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy import text
import eth_event
from pyhocon import ConfigFactory



import argparse

parser = argparse.ArgumentParser(description='Parse a contract on the Ethereum blockchain and store logs on a database.')
parser.add_argument('contract', 
                    help='name of the contract to parse like makermcd.vat (<schema>.<contract>)')

args = parser.parse_args()
schema, contract_name = args.contract.split(".")

print(f"Parsing contract {schema}.{contract_name}")

conf = ConfigFactory.parse_file('config.conf')

## Environment parameters
infura_key = conf["infura_key"]
fromBlock = conf["contracts"][schema][contract_name]["creationBlock"]

# Number of block per call (too big can lead to errors, to low can be too long)
blocksStep = conf.get(f"contracts.{schema}.{contract_name}.blocksStep", conf["blocksStep"])


## Input parameters
db_host = conf["db.host"]
db_user = conf["db.user"]
db_password = conf["db.password"]
db_db = conf["db.database"]

addresses = conf["contracts"][schema][contract_name]["addresses"]

# Loading the ABI json file
with open(f"conf/{schema}/{contract_name}.abi") as f:
  abi = json.load(f)

# Connect to PostgreSQL
engine = create_engine('postgresql://'+db_user+':'+db_password+'@'+db_host+':5432/'+db_db) 



# Set w3 source to Infura Mainnet
w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/' + infura_key)) 



# Set VAT contract address https://etherscan.io/address/0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b#code
addresses = [w3.toChecksumAddress(a) for a in addresses]

# template contract (the first address)
contract = w3.eth.contract(address=addresses[0], abi=abi)

# add tablename to abi json 
dict_fn = {} ## Manage an index for disambuguation of functions with same names but different signature
dict_evt = {} ## Manage an index for disambuguation of events  with same names but different signature
dict_sign = {} # function /events signature to item (function or event) abi

for j in abi:
  if j["type"] == "function" and j["stateMutability"] != "view":
    fn_name = j["name"].lower()
    signature = '{}({})'.format(j['name'],','.join([input['type'] for input in j['inputs']]))
    # Functions signature use the 4 first bytes of the sha3 then 0
    j["signature"] = w3.sha3(text=signature)[0:4].hex() + '00000000000000000000000000000000000000000000000000000000'
    # print(f"{j['name']}   {signature}   {j['signature']}")
    # If the name already exists, we add an index starting by 0 at the end of the function
    if fn_name in dict_fn:
      j["table"] = contract_name + "_call_" + fn_name + str(dict_fn[fn_name])
      dict_fn[fn_name] = dict_fn[fn_name]+1
    else:
      j["table"] = contract_name + "_call_" + fn_name
      dict_fn[fn_name] = 0
    dict_sign[j["signature"]] = j
  elif j["type"] == "event" and j["anonymous"] != True:
    j["signature"] = eth_event.get_log_topic(j)
    fn_name = j["name"].lower()
    # If the name already exists, we add an index starting by 0 at the end of the function
    if fn_name in dict_evt:
      j["table"] = contract_name + "_evt_" + fn_name + str(dict_evt[fn_name])
      dict_evt[fn_name] = dict_evt[fn_name]+1
    else:
      j["table"] = contract_name + "_evt_" + fn_name
      dict_evt[fn_name] = 0
    dict_sign[j["signature"]] = j

# print(abi)
# print(dict_sign)
# Initialize

# create all tables if needed

common_columns = "block_number bigint, block_hash bytea, address bytea, log_index int, transaction_index int, transaction_hash bytea"

type_mapping = {"address": "bytea", "bytes": "bytea", "bytes4": "bytea", "bytes32": "bytea", "int256": "numeric", "uint256": "numeric"}

with engine.connect() as sql:
  sql.execute(text(f"create schema if not exists {schema}"))
  for j in abi:
    if (j["type"] == "function" and j["stateMutability"] != "view") or (j["type"] == "event" and j["anonymous"] != True):
      table_name = j['table']
      sql_check_table_exists = f"select count(*) from information_schema.tables where table_schema = '{schema}' and table_name = '{table_name}'"
      if sql.execute(text(sql_check_table_exists)).scalar() == 0:
        columns = common_columns
        unnamed_col_idx = 0
        for i in j["inputs"]:
          col_name = i["name"].lower()
          if col_name == "":
            col_name = f"v{unnamed_col_idx}"
            unnamed_col_idx += 1
          columns += ', "'+col_name+'"' + " " + type_mapping[i["type"]]
        sql_create_table = f"""create table {schema}."{table_name}" ( {columns} )"""
        print(sql_create_table)
        sql.execute(text(sql_create_table))


# find the last blocknumber in the database for this contract
with engine.connect() as sql:
  for j in abi:
    if (j["type"] == "function" and j["stateMutability"] != "view") or (j["type"] == "event" and j["anonymous"] != True):
      table_name = j['table']
      sql_check_table_exists = f"""select max(block_number) from {schema}."{table_name}" """
      max_block = sql.execute(text(sql_check_table_exists)).scalar()
      if max_block != None and max_block > fromBlock:
        fromBlock = max_block + 1

print(f"Start from block {fromBlock}")


# List of all Maker contracts: https://etherscan.io/accounts/label/maker?subcatid=undefined&size=25&start=0&col=1&order=asc
# List of all Maker contracts in case you need it. https://github.com/duneanalytics/abstractions/blob/master/ethereum/makermcd/collateral_addresses.sql

#print(contract.all_functions())
#print(contract.events())

lastBlock = w3.eth.block_number

while fromBlock < lastBlock:
  toBlock = fromBlock + blocksStep
  if(toBlock > lastBlock):
    toBlock = lastBlock
  print(f"Fetching events from block {fromBlock} to {toBlock}")
  cnt = 0
  # Make sure we treat block as atomic so even if it crashes, we only have a full block or non
  with sessionmaker(engine).begin() as session: 
    for address in addresses:
      # TODO: manage to many results errors and manage the number of blocks automatically
      for t in w3.eth.get_logs({'fromBlock': fromBlock, 'toBlock': toBlock, 'address': address}):

        # CHeck if there is an ABI for such event
        try:
          j = dict_sign[t.topics[0].hex()]
        except KeyError:
          continue # Otherwise skip without error
        
        table_name = j["table"]
        values = ""
        
        if j["type"] == "function" and j["stateMutability"] != "view":
          inputs = contract.decode_function_input('0x' + t['data'][130:])
          params = inputs[1].values()
          for idx, value in enumerate(params):
            if j["inputs"][idx]["type"] == "address": # Addresses are giving in string but converted to binary array for space considerations
              values += ", '\\" + value[1:] + "'"
            elif isinstance(value, str):
              values += ", '" + str(value) +"'"
            elif isinstance(value, bytes):
              values += ", '\\x" + value.hex() + "'"
            else:
              values += ", " + str(value)
        elif j["type"] == "event" and j["anonymous"] != True:
          event_data = eth_event.decode_log(t, eth_event.get_topic_map(abi))
          for idx, event_param in enumerate(event_data["data"]):
            value = event_param["value"]
            if j["inputs"][idx]["type"] == "address": # Addresses are giving in string but converted to binary array for space considerations
              values += ", '\\" + value[1:] + "'"
            elif isinstance(value, str):
              values += ", '" + str(value) +"'"
            elif isinstance(value, bytes):
              values += ", '\\x" + value.hex()[1:]+ "'"
            else:
              values += ", " + str(value)
        else:
          continue

        # block_number bigint, block_hash bytea, address text, log_index int, transaction_index int, transaction_hash bytea"
        ## Preprend the common columns
        values = f"{t.blockNumber}, '\\{t.blockHash.hex()[1:]}', '\\{t.address[1:]}', {t.logIndex}, {t.transactionIndex}, '\\{t.transactionHash.hex()[1:]}' {values}"

        sql_insert = f"""insert into {schema}."{table_name}" values ({values})"""
        # print(sql_insert)
        session.execute(text(sql_insert))
        cnt += 1
  print(f"Inserted {cnt} lines")
  fromBlock = toBlock + 1 

