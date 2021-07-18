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
import snowflake.connector as sf

from functions import get_abi, get_abi_params


# Allow user to input contract in terminal command
parser = argparse.ArgumentParser(description='Parse a contract on the Ethereum blockchain and store logs on a database.')
parser.add_argument('contract', help='name of the contract to parse like makermcd.vat (<schema>.<contract>)')
args = parser.parse_args()

schema, contract_name = args.contract.split(".")
print(f"Parsing contract {schema}.{contract_name}")

conf = ConfigFactory.parse_file('config.conf')


## Environment parameters
infura_key = conf["infura_key"]
try:
  fromBlock = conf["contracts"][schema][contract_name]["creationBlock"]
except:
  print("Did you add the file path to config.conf?")
  raise


# Number of blocks per call (too big can lead to errors, too low can be too long)
blocksStep = conf.get(f"contracts.{schema}.{contract_name}.blocksStep", conf["blocksStep"])

## Input parameters
db_host = conf["db.host"]
db_user = conf["db.user"]
db_password = conf["db.password"]
db_db = conf["db.database"]
addresses = conf["contracts"][schema][contract_name]["addresses"]
snowflake_user = conf["snowflake.user"]
snowflake_pwd = conf["snowflake.password"]
snowflake_account = conf["snowflake.account"]
snowflake_wh = conf["snowflake.warehouse"]

print(addresses)
  
# Connect to PostgreSQL
engine = create_engine('postgresql://'+db_user+':'+db_password+'@'+db_host+':5432/'+db_db) 


# Convert each event's input data to a readable format. Then decode it.
def get_function_data(t):
  x=2
  inputs = None

  if t['data'] == '0x':
    print("The input data coming from Infura is empty.")
    inputs = []
    params =[]

  else:
    while inputs is None:
      try:
        input_data = '0x' + t['data'][x:]
        inputs = contract.decode_function_input(input_data)
        params = inputs[1].values()

        #print(inputs)
        
      except ValueError:    
        x += 8 #or x+=32. NOTE: This removes leading topics (0s) from the input data. Works well in multiples of 8 or 16.
        
        if input_data == '0x': #If the string is never able to be read 'decode_function_input' (and it just truncates to 0x)
          print('Cannot read input data. The input data or ABI may be invalid.', t['data'])
          x=2
          raise ValueError # Do I need to raise an error here?

        pass

  methodid = input_data[:10] + '00000000000000000000000000000000000000000000000000000000'
  return inputs, params, methodid
  

def create_schema (abi):
  # create all tables if needed
  common_columns = "block_number bigint, block_hash bytea, address bytea, log_index int, transaction_index int, transaction_hash bytea"
  type_mapping = {"address": "bytea", "bytes": "bytea", "bytes4": "bytea", "bytes32": "bytea", "int256": "numeric", "uint256": "numeric", "uint16":"numeric", "bool": "boolean", "address[]":"bytea", "uint256[]":"numeric", "uint8":"numeric", "string":"bytea"}

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
              print("sql columns:", columns)
            except KeyError:
              print("There is probably an unsupported datatype You can add more to the type_mapping dict above")
              raise
          sql_create_table = f"""create table {schema}."{table_name}" ( {columns} )"""
          print(sql_create_table)
          sql.execute(text(sql_create_table))


'''
Are the column types below the most efficient ?
'''
def create_schema_snowflake (abi):
  # create all tables if needed
  common_columns = "block_number bigint, block_hash varchar, address varchar, log_index int, transaction_index int, transaction_hash varchar"
  type_mapping = {"address": "varchar", "bytes": "varchar", "bytes4": "varchar", "bytes32": "varchar", "int256": "numeric", "uint256": "numeric", "uint16":"numeric", "bool": "boolean", "address[]":"varchar", "uint256[]":"numeric", "uint8":"numeric", "string":"varchar"}

  for j in abi:
      # Collect functions and events from the ABI
      if (j["type"] == "function" and j["stateMutability"] != "view") or (j["type"] == "event" and j["anonymous"] != True):
        table_name = j['table']
        columns = common_columns
        unnamed_col_idx = 0
        for i in j["inputs"]:
          col_name = i["name"].lower()
          if col_name == "":
            col_name = f"v{unnamed_col_idx}"
            unnamed_col_idx += 1
          try:
            columns += ', "'+col_name+'"' + " " + type_mapping[i["type"]] #map the type from the ABI to the sql type in the 'type_mapping' dict
          except KeyError:
            print("There is probably an unsupported datatype You can add more to the type_mapping dict above")
            raise

        # Create or replace table
        conn.cursor().execute(f"CREATE OR REPLACE TABLE {table_name}({columns})") #columns format: (col1 integer, col2 string, ...)



# Return transaction logs or filter the specific type of log you need to return.
def read_logs(address, fromBlock, toBlock):   

  # If proxy_actions, only find event/function logs that are sent to DSSProxyActions (0x82ecd135dce65fbc6dbdd0e4237e0af93ffd5038)
  # These can also be found as '0 value internal transactions' on Etherscan, but there is no API calls for it at the moment. Is there a faster way?
  if contract_name == 'proxy_actions':
    t = []
    for logs in w3.eth.get_logs({'fromBlock': fromBlock, 'toBlock': lastBlock, 'address': address}):
      receipt = w3.eth.getTransactionReceipt(logs.transactionHash)['logs'][0]#['data']
      if '82ecd135dce65fbc6dbdd0e4237e0af93ffd5038' in receipt.data:
        t.append(receipt) #Can you make this faster? It seems pretty slow...
        print("Retrieving logs before inserting. This may take a minute.", len(t))

    return t
      
  #If we're not finding events from proxy_actions, just read logs normally
  else:
    t = w3.eth.get_logs({'fromBlock': fromBlock, 'toBlock': toBlock, 'address': address})
    return t


# Set w3 source to Infura Mainnet and init contract
w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/' + infura_key)) 

# Get addresses, contract names, and ABI parameters
for address in addresses: # This only gets the last address. Get all. I think you have to put this under 'for address in addresses' below. ***
  address, abi = get_abi(address, schema, contract_name)

addresses = [w3.toChecksumAddress(a) for a in addresses] # Get addresses
contract = w3.eth.contract(address=addresses[0], abi=abi) # Get contracts
j, dict_evt, dict_fn, dict_sign = get_abi_params(abi, contract_name, w3) # Get ABI parameters (function names, event names, etc.) 
create_schema(abi) # Create SQL Schema if it doesn't already exist


# Snowflake
conn = sf.connect(
user= snowflake_user, #userid
password= snowflake_pwd, #password
account= snowflake_account, #organization_name.account_name
warehouse =  snowflake_wh # You first need to create a warehouse and save the name in template.conf
)

conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS makerdw_dev")
conn.cursor().execute(f"USE DATABASE makerdw_dev") 
conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
conn.cursor().execute(f"USE SCHEMA {schema}")

create_schema_snowflake(abi)


# Start Reading transactions
print(f"Start from block {fromBlock}")
lastBlock = w3.eth.block_number

# find the last blocknumber in the database for this contract
with engine.connect() as sql:
  for j in abi:
    if (j["type"] == "function" and j["stateMutability"] != "view") or (j["type"] == "event" and j["anonymous"] != True):
      table_name = j['table']
      sql_check_table_exists = f"""select max(block_number) from {schema}."{table_name}" """
      max_block = sql.execute(text(sql_check_table_exists)).scalar()
      if max_block != None and max_block > fromBlock:
        fromBlock = max_block + 1


# Fetch event data from each block
while fromBlock < lastBlock:
  toBlock = fromBlock + int(blocksStep)
  if(toBlock > lastBlock):
    toBlock = lastBlock
  print(f"Fetching events from block {fromBlock} to {toBlock}")
  print("BlockStep:", blocksStep)
  cnt = 0

  # Make sure we treat block as atomic so even if it crashes, we only have a full block or non
  with sessionmaker(engine).begin() as session: 
    for address in addresses:
      
      # TODO: manage too many results errors and manage the number of blocks automatically
      for t in read_logs(address, fromBlock, toBlock): 

        # Construct j based on the topic found in t (methodid)
        try:
          j = dict_sign[t.topics[0].hex()]
          table_name = j["table"] 
          values = ""
        except KeyError:
          pass

        #Decode the input data
        if j["type"] == "function" and j["stateMutability"] != "view":
          try:
            inputs, params, methodid = get_function_data(t) 
            #print("inputs:", inputs, "\n params:", params)
          except:
            print("Could not parse input data")
            raise ValueError("Your input data is probably not readable") #Should we pass here instead?
         
          # Replace j if the input data contains the signature/methodid instead of topic 0 ('execute' transactions).
          if contract_name == 'proxy_actions':
            j = dict_sign[methodid]
            table_name = j["table"]
            values = ""

          # Encode functions for SQL
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
            print('Could not encode parameters', params)
            print('type1', type(params[0]), 'type2', type(params[1]), 'type3', type(params[2]))
            continue #If it can't encode it, is it okay to write it as it is?

        # Encode events for SQL  
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
        #sql_insert = f"""insert into {schema}."{table_name}" values ({values})"""
        #print(text(sql_insert))
        #session.execute(text(sql_insert)) # Uncomment when you want to submit or post to the database

        # SNOWFLAKE
        conn.cursor().execute(f"INSERT INTO {table_name} VALUES ({values})")
        print(f"INSERT INTO {table_name} VALUES ({values})")
  
      # Manage the blockstep automatically
      if cnt == 0: #If there are 0 insertions in a blockstep, increase the block step
        blocksStep = blocksStep*1.2
      if cnt > 30: # If there are > 30 insertions in one blockstep, decrease the blockstep.
        blocksStep = blocksStep/2
      cnt += 1    

  fromBlock = toBlock + 1 
  print(f"Inserted {cnt} lines into {schema}.{table_name}") # Sometimes there are no insertions when this is printed.


#NOTES
# 1. Is proxy_actions too slow? If so, how can we speed this up?
# 2. double check flashloan_call (mutable ?)
# 3. ethereum.transactions
# 4. handle the 'too many transactions' error


'''   
#ethereum.transactions NOTE: This does not work yet
if addresses == []:
  print("Getting all transaction logs") #If the address list is empty, get all transactions
  table_name = 'transactions'
  # Potentially run a unique function and then exit
  
  sql_check_table_exists = f"""select max(block_number) from {schema}."{table_name}" """
  with engine.connect() as sql:
    # Check if table exists. If not, create table 
    sql_check_table_exists = f"select count(*) from information_schema.tables where table_schema = '{schema}' and table_name = '{table_name}'"
    if sql.execute(text(sql_check_table_exists)).scalar() == 0:  
      columns =  "block_number bigint, block_hash bytea, address bytea, log_index int, transaction_index int, transaction_hash bytea"
      sql_create_table = f""" create table {schema}."{table_name}" ( {columns} )"""
      print(sql_create_table, 'created table')
      sql.execute(text(sql_create_table))
  # Read in blocks
  abi = []
  print('fromblock', fromBlock)
  toBlock = fromBlock + 1
  # Sample response: AttributeDict({'address': '0x875773784Af8135eA0ef43b5a374AaD105c5D39e', 'blockHash': HexBytes('0x500e9107ee7757e683b8420f94897dcbee789761fe0949f5f9374c262afc8725'), 'blockNumber': 12801059, 'data': '0x00000000000000000000000000000000000000000000002a9bf0f397f1080000', 'logIndex': 0, 'removed': False, 'topics': [HexBytes('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'), HexBytes('0x000000000000000000000000031f71b5369c251a6544c41ce059e6b3d61e42c6'), HexBytes('0x000000000000000000000000275da8e61ea8e02d51edd8d0dc5c0e62b4cdb0be')], 'transactionHash': HexBytes('0x558a88ca3fe10e02de0730844f6ae55708f0b498b9527257783be0986d17d995'), 'transactionIndex': 0})
'''
