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
import warnings
import requests
import time

from functions import get_abi


# Allow user to input contract in terminal command
parser = argparse.ArgumentParser(description='Parse a contract on the Ethereum blockchain and store logs on a database.')
parser.add_argument('contract', help='name of the contract to parse like makermcd.vat (<schema>.<contract>)')
args = parser.parse_args()

schema, contract_name = args.contract.split(".")
print(f"Parsing contract {schema}.{contract_name}")

conf = ConfigFactory.parse_file('config.conf')


## Environment parameters
infura_key = conf["infura_key"]
fromBlock = conf["contracts"][schema][contract_name]["creationBlock"]
#fromBlock = 12736124

# Number of block per call (too big can lead to errors, too low can be too long)
blocksStep = conf.get(f"contracts.{schema}.{contract_name}.blocksStep", conf["blocksStep"])

## Input parameters
db_host = conf["db.host"]
db_user = conf["db.user"]
db_password = conf["db.password"]
db_db = conf["db.database"]
addresses = conf["contracts"][schema][contract_name]["addresses"]
print(addresses)


# I think you have to put this under 'for address in addresses' below. 
for address in addresses: # This only gets the last address. Get all
  address, abi, contract_name = get_abi(address, schema, contract_name)
  
# Connect to PostgreSQL
engine = create_engine('postgresql://'+db_user+':'+db_password+'@'+db_host+':5432/'+db_db) 

# Set w3 source to Infura Mainnet and init contract
w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/' + infura_key)) 
addresses = [w3.toChecksumAddress(a) for a in addresses]
contract = w3.eth.contract(address=addresses[0], abi=abi)

# add tablename to abi json 
dict_fn = {} ## Manage an index for disambuguation of functions with same names but different signature
dict_evt = {} ## Manage an index for disambuguation of events  with same names but different signature
dict_sign = {} # function /events signature to item (function or event) abi


def get_function_data(t):
  # Convert each event's input data to a readable format. Try multiples of 2 + 32: e.g. 34, 66, 98, 130, etc.
  x=2
  inputs = None

  while inputs is None:
    try:
      #print(t['transactionHash'])
      input_data = '0x' + t['data'][x:]
      inputs = contract.decode_function_input(input_data)
      params = inputs[1].values()
      #print(inputs)
      
    except ValueError:    #If you get the 'could not find any function with matching selector' error, then the input data is incorrectly formatted for 'decode_function_input'. This may happen when there are too many topic fields. Thise truncates the input data to make it accepted by the decode_input_function.
      x = x+32 #or x=x+1 if that doesn't work?
      #print(input_data)

      if input_data == '0x': #If the string is never able to be read 'decode_function_input' (and it just truncates to 0x)
        print('Cannot read input data. The following input data may be invalid.' + t['data'])
        x=2
        #inputs = 1
        
      pass
      
  return inputs, params

#From the ABI, get function and event names j['name']
def get_abi_params(abi):
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
  return j

# Initialize

def create_schema (abi):
  # create all tables if needed
  common_columns = "block_number bigint, block_hash bytea, address bytea, log_index int, transaction_index int, transaction_hash bytea"
  type_mapping = {"address": "bytea", "bytes": "bytea", "bytes4": "bytea", "bytes32": "bytea", "int256": "numeric", "uint256": "numeric", "uint16":"numeric", "bool": "boolean", "address[]":"bytea", "uint256[]":"numeric"}

  with engine.connect() as sql:
    sql.execute(text(f"create schema if not exists {schema}")) 
    for j in abi:
      # Collect all functions and events from the ABI (I think)
      if (j["type"] == "function" and j["stateMutability"] != "view") or (j["type"] == "event" and j["anonymous"] != True):
        table_name = j['table']
        # Create an SQL table for each event/function if it doesn't already exist
        sql_check_table_exists = f"select count(*) from information_schema.tables where table_schema = '{schema}' and table_name = '{table_name}'"
        if sql.execute(text(sql_check_table_exists)).scalar() == 0:
          columns = common_columns
          unnamed_col_idx = 0
          for i in j["inputs"]:
            col_name = i["name"].lower()
            print("colname:", col_name)
            if col_name == "":
              col_name = f"v{unnamed_col_idx}"
              unnamed_col_idx += 1
            try:
              columns += ', "'+col_name+'"' + " " + type_mapping[i["type"]] #map the type from the ABI to the sql type in the 'type_mapping' dict
            except KeyError:
              print("There is probably an unsupported datatype You can add more to the type_mapping dict above")
              raise
          sql_create_table = f"""create table {schema}."{table_name}" ( {columns} )"""
          print(sql_create_table)
          sql.execute(text(sql_create_table))

# Get ABI parameters
j = get_abi_params(abi)
#print('this is j', j, j['table'], j['type'], j['name'])

# Create SQL Schema (if it doesn't already exist)
create_schema(abi)

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

lastBlock = w3.eth.block_number


# Fetch event data from each block
while fromBlock < lastBlock:
  toBlock = fromBlock + blocksStep
  if(toBlock > lastBlock):
    toBlock = lastBlock
  print(f"Fetching events from block {fromBlock} to {toBlock}")
  cnt = 0
  # Make sure we treat block as atomic so even if it crashes, we only have a full block or non
  with sessionmaker(engine).begin() as session: 
    for address in addresses:
      # TODO: manage too many results errors and manage the number of blocks automatically
      for t in w3.eth.get_logs({'fromBlock': fromBlock, 'toBlock': toBlock, 'address': address}):
        print(t)

        # Check if there is an event in the existing contract's ABI for this log
        try:
          j = dict_sign[t.topics[0].hex()]
        # If there isn't, this may be a proxy contract. Check to see if we have a proxy contract/abi. If not, get it.
        except KeyError:
          print('Topic was not found in the ABI')
          pass

        table_name = j["table"] #Set table_name equal to the key of 'topics[0]' in the ABI (I think)
        values = ""
  
        if j["type"] == "function" and j["stateMutability"] != "view": #Read functions that change blockchain state
          # Decode the input data

          try:
            inputs, params = get_function_data(t) 
            print("inputs:", inputs, "\n params:", params)
          except:
            pass

          # Encode parameters
          try:
            for idx, value in enumerate(params):
              #if IndexError, list index out of range, then it's bc/ one of your params has an additional value in it ... figure out how these should be handled
              if j["inputs"][idx]["type"] == "address": # Addresses are given as string but converted to binary array for space considerations
                values += ", '\\" + value[1:] + "'"
              elif isinstance(value, str): # returns true if value is a string
                values += ", '" + str(value) +"'"
              elif isinstance(value, bytes):
                values += ", '\\x" + value.hex() + "'"
              else:
                values += ", " + str(value)
          except:
            print('Could not parse', params)
            continue #If it can't parse, just skip it. Is that okay?

        #if j["type"] == "function" and j["stateMutability"] == "view":
        #  print("this isnt reading certain mutable functions?")

        # Decode events      
        elif j["type"] == "event" and j["anonymous"] != True:
          event_data = eth_event.decode_log(t, eth_event.get_topic_map(abi))
          
          for idx, event_param in enumerate(event_data["data"]):
            value = event_param["value"]
            if j["inputs"][idx]["type"] == "address": # Addresses are given in string but converted to binary array for space considerations
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
        print(text(sql_insert))

        #session.execute(text(sql_insert)) # *** Uncomment when you want to submit or post to the database

        cnt += 1

  print(f"Inserted {cnt} lines into {schema}.{table_name}")
  fromBlock = toBlock + 1 


#NOTES
# 2. double check flashloan_call and other mutable functions (that they're actually reading+writing)
# 3. 
