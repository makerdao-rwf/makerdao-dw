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

# Loading the ABI json file
with open(f"conf/{schema}/{contract_name}.abi") as f:
  abi = json.load(f)

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

        # Check if there is an ABI for such event
        try:
          j = dict_sign[t.topics[0].hex()]
        except KeyError:
          continue # Otherwise skip without error
        
        table_name = j["table"]
        values = ""

        if j["type"] == "function" and j["stateMutability"] != "view":

          #### ADDED #### Convert each event's input data to a readable format. Try multiples of 32 + 2 ? e.g. 34, 66, 98, 130, 162, 194, 226, 258, 290, etc.
          x=2
          inputs = None
          print(t['data'])

          while inputs is None:
            try:
              input_data = '0x' + t['data'][x:]
              #print(input_data)
              inputs = contract.decode_function_input(input_data)
              params = inputs[1].values()
              print(inputs)
            except ValueError:    #If you get the 'could not find any function with matching selector' error, then the input data is incorrectly formatted for 'decode_function_input'. This may happen when there are a large number of topic fields. The below code truncates the input data to make it accepted by the decode_input_function.
              x = x+32 #or x=x+1 if that doesn't work?
              if input_data == '0x': #If the string is never able to be read by 'decode_function_input' (and it just truncates to 0x), then it may be a proxy contract
                
                ### ADDED ###
                #try:
                  #Make this all a single function
                #  tx_input_data = w3.eth.get_transaction(t['transactionhash'])
                #  tx_input_data = str(tx_input_data['input']) #The log's data in topic[0] (e.g. 0x5b8f46461c1dd69fb968f1a003acee221ea3e19540e350233b612ddb43433b55) cannot be decoded to the methodid because this is a keccak encoding of the methodid
                #  event_input_data = tx_input_data[:10] + t['data'][2:]
                #  print(event_input_data)
                  # Here, you need to try many different ABIs
                #  decoded = contract.decode_function_input(input_data)
                #except ValueError:
                #  pass
                
                inputs = 'cannot read input data'
                print(inputs)
                x=2
              pass

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
