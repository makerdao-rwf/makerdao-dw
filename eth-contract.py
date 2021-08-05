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
from sqlalchemy.dialects import registry
registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')

from functions import get_abi, get_abi_params, get_function_data


# Allow user to input contract in terminal command
parser = argparse.ArgumentParser(description='Parse a contract on the Ethereum blockchain and store logs on a database.')
parser.add_argument('contract', help='name of the contract to parse like makermcd.vat (<schema>.<contract>)')
args = parser.parse_args()

schema, contract_name = args.contract.split(".")
print(f"Parsing contract {schema}.{contract_name}")

conf = ConfigFactory.parse_file('config.conf')


## Environment parameters
infura_key = conf["infura_key"]
creationBlock = conf["contracts"][schema][contract_name]["creationBlock"]

# Number of blocks per call (too big can lead to errors, too low can be too long)
blocksStep = conf.get(f"contracts.{schema}.{contract_name}.blocksStep", conf["blocksStep"])

## Input parameters
db_host = conf["db.host"]
db_user = conf["db.user"]
db_password = conf["db.password"]
db_db = conf["db.database"]
db_port = conf["db.port"]
db_driver = conf["db.driver"] # snowflake or postgresql 
addresses = conf["contracts"][schema][contract_name]["addresses"]

print("Addresses: ", addresses)



class SqlEngine:

  # engine var should be defined by the subclasses otherwise it will crash
  # common_columns var should be defined by the subclasses otherwise it will crash
  # type_mapping var should be defined by the subclasses otherwise it will crash
  
  def __init__(self, abi):
    self.abi = abi

  # TO BE DEFINED in the implementation
  def connect(self):
    return self.engine.connect()

  ## Start a transaction - TO BE DEFINED in the implementation
  def begin(self):
    return sessionmaker(self.engine).begin()

  ## Start a transaction - TO BE DEFINED in the implementation
  def execute(self, sql):
    return self.engine.execute(sql)

  def get_latest_block (self, fromBlock):
    '''find the last blocknumber in the database for this contract. NOTE: DO with eth-blocks afterwards'''
    with self.engine.connect() as sql:
      for j in abi:
        if (j["type"] == "function" and j["stateMutability"] != "view") or (j["type"] == "event" and j["anonymous"] != True):
          table_name = j['table']
          sql_check_table_block = f"""select max(block_number) from {self.db}.{schema}."{table_name}" """ # DBL CHECK. UPPER

          try: # Added this try/except. If there isn't a table, then just start at the beginning. IS THERE A NEED FOR eth-blocks.py????
            max_block = self.execute(text(sql_check_table_block)).scalar()
            if max_block != None and max_block >= fromBlock:
              fromBlock = max_block + 1
          except:
            fromBlock = conf["contracts"][schema][contract_name]["creationBlock"]
      
      return fromBlock

  def create_schema (self):
    with self.connect() as sql: # NOTE: ONLY DO THIS ONCE ?
      #sql.execute(text(f"create schema if not exists {schema}")) #Does this need to be here? I don't think it does...
      for j in abi:
        # Collect all functions and events from the ABI (I think)
        if (j["type"] == "function" and j["stateMutability"] != "view") or (j["type"] == "event" and j["anonymous"] != True):
          table_name = j['table']
          # Create an SQL table for each event/function if it doesn't already exist
          sql_check_table_exists = f"select exists(select * from {self.db}.information_schema.tables where table_schema = '{schema.lower()}' or table_schema = '{schema.upper()}' and table_name = '{table_name}')" #put this on blcok thing above
          if self.execute(text(sql_check_table_exists)).scalar() == False:
            columns = self.common_columns
            unnamed_col_idx = 0
            for i in j["inputs"]:
              col_name = i["name"].lower()
              if col_name == "":
                col_name = f"v{unnamed_col_idx}"
                unnamed_col_idx += 1
              try:
                columns += ', "'+col_name+'"' + " " + self.type_mapping[i["type"]] #map the type from the ABI to the sql type in the 'type_mapping' dict
                print("sql columns:", columns)
              except KeyError:
                print("There is probably an unsupported datatype You can add more to the type_mapping dict above")
                raise
            sql_create_table = f"""create table if not exists {schema}."{table_name}" ( {columns} )""" 
            print(sql_create_table)
            sql.execute(text(sql_create_table))
          else:
            print('Tables already exist')
            break
        


class SnowflakeEngine(SqlEngine):
  def __init__ (self, abi, host, user, password, database, port):
    super.__init__(abi)
    self.engine = create_engine(f'snowflake://{user}:{password}@{host}:{port}/{database}')
    self.common_columns = "block_number bigint, block_hash string, address string, log_index int, transaction_index int, transaction_hash string"
    self.type_mapping = {"address": "string", "bytes": "string", "bytes4": "string", "bytes32": "string", "int256": "numeric", "uint256": "string", "uint16":"numeric", "bool": "boolean", "address[]":"string", "uint256[]":"string", "uint8":"numeric", "string":"string"} #NOTE: I changed uint256 and uint256[] to string...

  def encode_functions(self, params, values):
    '''Encode function parameters for Snowflake'''   
    for idx, value in enumerate(params):
      if isinstance(value, bytes):
        values += ", '" + value.hex() + "'"
      else:
        values += ", " + "'" + str(value)+ "'"

    return values

  def encode_events(self, event_data, values):
    '''Encode event parameters for Snowflake'''
    for idx, event_param in enumerate(event_data["data"]):
      value = event_param["value"]
      if isinstance(value, bytes):
        values += ", '" + value.hex() + "'"
      else:
        values += ", " + "'" + str(value)+ "'"

    return values

  def insert(self, values):
    '''Insert values into Snowflake'''
    # Prepend common columns
    values = f"{t.blockNumber}, '{t.blockHash.hex()}', '{t.address}', {t.logIndex}, {t.transactionIndex}, '{t.transactionHash.hex()}' {values}"
    sql_insert = f"""insert into {schema}."{table_name}" values ({values})"""
    print(text(sql_insert))
    session.execute(text(sql_insert))



class PostgresqlEngine(SqlEngine):
  def __init__ (self, abi, host, user, password, database, port):
    super.__init__(abi)
    self.engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    self.common_columns = "block_number bigint, block_hash bytea, address bytea, log_index int, transaction_index int, transaction_hash bytea"
    self.type_mapping = {"address": "bytea", "bytes": "bytea", "bytes4": "bytea", "bytes32": "bytea", "int256": "numeric", "uint256": "numeric", "uint16":"numeric", "bool": "boolean", "address[]":"bytea", "uint256[]":"numeric", "uint8":"numeric", "string":"bytea"}


  def encode_functions(self, params, values):
    '''Encode Function parameters for Postgresql''' 
    for idx, value in enumerate(params):    
      if j["inputs"][idx]["type"] == "address": # Addresses are given as string but converted to binary array for space considerations
        values += ", '\\" + value[1:] + "'"
      elif isinstance(value, str): # returns true if value is a string
        values += ", '" + str(value) +"'"
      elif isinstance(value, bytes):
        values += ", '\\x" + value.hex() + "'"
      else:
        values += ", " + str(value)
    return values

  def encode_events(self, event_data, values):
    '''Encode Event parameters for Postgresql'''
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
    return values

  def insert(self, values):
    '''Insert values into Postgresql'''
    values = f"{t.blockNumber}, '\\{t.blockHash.hex()[1:]}', '\\{t.address[1:]}', {t.logIndex}, {t.transactionIndex}, '\\{t.transactionHash.hex()[1:]}' {values}"
    sql_insert = f"""insert into {schema}."{table_name}" values ({values})"""
    print(text(sql_insert))
    session.execute(text(sql_insert))

def create_engine(abi, db_driver, db_host, db_user, db_password, db_db, db_port):
  if db_driver == "snowflake":
    return SnowflakeEngine(abi, db_driver, db_host, db_user, db_password, db_db, db_port)
  elif db_driver == "postgresql":
   return PostgresqlEngine(abi, db_host, db_user, db_password, db_db, db_port)
  else:
   return None
  

# Return transaction logs or filter the specific type of log you need to return. NOTE: VERIFY THAT THIS IS NOT RETURNING DUPLICATES AND THAT IT'S NOT REMOVING ANYTHING THAT SHOULD BE KEPT
def read_logs(address, fromBlock, toBlock):   

  # If proxy_actions, only find event/function logs that are sent to DSSProxyActions (0x82ecd135dce65fbc6dbdd0e4237e0af93ffd5038)
  if contract_name == 'proxy_actions':
    t,duplicates = [],[]

    # NOTE: THIS MAY BE QUICKER: https://infura.io/docs/ethereum/json-rpc/eth-getTransactionReceipt dont think so actually, its the same thing i think

    for log in w3.eth.get_logs({'fromBlock': fromBlock, 'toBlock': toBlock, 'address': address}):
      if log.transactionHash.hex() not in duplicates: #remove duplicate transactions
        duplicates.append(log.transactionHash.hex())
        receipt = w3.eth.getTransactionReceipt(log.transactionHash)['logs'][0]#['data'] #Is an infura request made for each transaction?
        if '82ecd135dce65fbc6dbdd0e4237e0af93ffd5038' in receipt.data: 
          t.append(receipt) #Can you make this faster?
          print("Retrieving log ", len(t))

    return t
      
  #If we're not finding events from proxy_actions, just read logs normally
  else:
    t = w3.eth.get_logs({'fromBlock': fromBlock, 'toBlock': toBlock, 'address': address})
    return t


# Set w3 source to Infura Mainnet and init contract
w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/' + infura_key)) 

address, abi = get_abi(addresses[0], schema, contract_name) #get ABI from the first address (it should work on all addresses).
addresses = [w3.toChecksumAddress(a) for a in addresses] # Get addresses
contract = w3.eth.contract(address=addresses[0], abi=abi) # Get contracts
j, dict_evt, dict_fn, dict_sign = get_abi_params(abi, contract_name, w3) # Get ABI parameters (function names, event names, etc.) 


engine = create_engine(abi, db_driver, db_host, db_user, db_password, db_db, db_port)
  # die with an exception


# Get latest block and create a schema
fromBlock = engine.get_latest_block(creationBlock)
engine.create_schema()


# Start Reading transactions
print(f"Start from block {fromBlock}")
lastBlock = w3.eth.block_number

# Fetch event data from each block
while fromBlock < lastBlock:
  toBlock = fromBlock + int(blocksStep)
  if(toBlock > lastBlock):
    toBlock = lastBlock
  print(f"Fetching events from block {fromBlock} to {toBlock}", "\n", "BlockStep:", blocksStep)
  cnt = 0

  # Make sure we treat block as atomic so even if it crashes, we only have a full block or none
  with engine.begin() as session: 
    for address in addresses:

      # Retrieve each log
      try:
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
              inputs, params, methodid = get_function_data(t, contract) 
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
              values = engine.encode_functions(params, values) #this probably shouldn't be in the sqlabstract class
            except:
              print('Could not encode parameters: \n','type1', type(params[0]), 'type2', type(params[1]), 'type3', type(params[2]))
              continue #CONTINUE IF IN LOOP. If it can't encode it, is it okay to write it as it is?

          # Encode events for SQL  
          elif j["type"] == "event" and j["anonymous"] != True:
            event_data = eth_event.decode_log(t, eth_event.get_topic_map(abi))
            values = engine.encode_events(event_data, values)
          else:
            continue
          
          # Insert values
          engine.insert(values)
          cnt += 1  

      # Manage the number of blocks returned by each an Infura query (blockstep) automatically
      except ValueError as VE: # If the Infura returns a 'too many logs error', decrease the blockstep
        blocksStep /= 2
        print(VE, "\n", "New BlockStep:", blocksStep)
        break
      
  if cnt == 0: #If there are 0 insertions in a blockstep, increase the blockstep for the next iteration.
     blocksStep *= 2
  if cnt > 50: # If there are > 50 insertions in one blockstep, decrease the blockstep.
     blocksStep /= 1.3  
      
  # Increase fromBlock for the next iteration
  fromBlock = toBlock + 1 

  # Commit is automatically called here bc/ the session has ended
  print(f"Inserted {cnt} lines into {engine.engine}")


#NOTES
# 1. Double check to see if you actually need eth-blocks.py ??
# 2. double check flashloan_call (mutable ?)
# 3. ethereum.transactions
# 5. Increase all blocksteps to reduce number of infura reads


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
