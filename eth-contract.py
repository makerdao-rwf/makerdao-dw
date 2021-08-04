from io import StringIO
from sqlalchemy.sql.expression import table
from web3 import Web3
import eth_event
import time
import requests
from sqlalchemy.dialects import registry
registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')

from functions import get_abi, get_abi_params, get_function_data, get_conf, get_schema_and_contract
from classes import *


# Get user input - schema, contract name, and database information (conf file)
schema, contract_name = get_schema_and_contract()
conf = get_conf()

## Environment parameters
infura_key = conf["infura_key"]
creationBlock = conf["contracts"][schema][contract_name]["creationBlock"]

# Number of blocks per call (too big can lead to errors, too low can be too long)
blocksStep = conf.get(f"contracts.{schema}.{contract_name}.blocksStep", conf["blocksStep"])

## Input parameters
db_driver = conf["db.driver"] # snowflake or postgresql
db_host = conf["db.host"]
db_user = conf["db.user"]
db_password = conf["db.password"]
db_db = conf["db.database"]
db_port = conf["db.port"] 
db_account = conf["db.account"]
db_warehouse = conf["db.warehouse"]
addresses = conf["contracts"][schema][contract_name]["addresses"]


print("Addresses: ", addresses)

# Initialize Infura and contract data
w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/' + infura_key))  # Set w3 source to Infura Mainnet
address, abi = get_abi(addresses[0], schema, contract_name) # Get ABI from the first address (it should work on all addresses).
addresses = [w3.toChecksumAddress(a) for a in addresses] # Get addresses
contract = w3.eth.contract(address=addresses[0], abi=abi) # Get contracts
j, dict_evt, dict_fn, dict_sign = get_abi_params(abi, contract_name, w3) # Get ABI parameters (function names, event names, etc.) 


# Return transaction logs or filter the specific type of log you need to return. NOTE: VERIFY THAT THIS IS NOT RETURNING DUPLICATES AND THAT IT'S NOT REMOVING ANYTHING THAT SHOULD BE KEPT
def read_logs(address, fromBlock, toBlock):   

  # If proxy_actions, only find event/function logs that are sent to DSSProxyActions (0x82ecd135dce65fbc6dbdd0e4237e0af93ffd5038)
  if contract_name == 'proxy_actions':
    t,duplicates = [],[]
    
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
    
# Create SQL Alchemy Engine
try:
  engine = start_engine(abi, db_driver, db_host, db_user, db_password, db_account, db_db, db_port)
except:
  print("Verify that template.conf is setup correctly. There should be no empty fields.")
  raise ValueError()

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
        start = time.time()
        for t in read_logs(address, fromBlock, toBlock): 
          end = time.time()
          print("TIME read_logs", end - start)

          # Construct j based on the topic found in t (methodid)
          try:
            j = dict_sign[t.topics[0].hex()]
            table_name = j["table"] 
            values = ""
          except KeyError:
            pass

          #If the transaction's signature is a function, decode the log.
          if j["type"] == "function" and j["stateMutability"] != "view":
            try:
              start2 = time.time()
              inputs, params, methodid = get_function_data(t, contract) #this might be able to be sped up.
              end2 = time.time()
              print("TIME get_function_data", end2 - start2)
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
              start4=time.time()
              engine.encode_functions(j, params, values) # Better to start with SqlEngine ?
              end4 = time.time()
              print("TIME encode events:", start4-end4)
            except:
              print('Could not encode parameters: \n','type1', type(params[0]), 'type2', type(params[1]), 'type3', type(params[2]))
              continue #CONTINUE IF IN LOOP. If it can't encode it, is it okay to write it as it is?

          # If the signature in t is an event, decode log and encode data for SQL  
          elif j["type"] == "event" and j["anonymous"] != True:
            event_data = eth_event.decode_log(t, eth_event.get_topic_map(abi)) #change event_data to params?
            engine.encode_events(j, event_data, values)
          else:
            continue
          
          # Insert values
          start3 = time.time()
          sql_insert = engine.insert(t, table_name, session) #SEVERELY IMPACTING SPEED. Is it faster to pass session?
          end3 = time.time()
          print("TIME insert:", end3 - start3)
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
# 1. Did we want to do the blockstep and everything for ethereum.transactions?
# 2. double check flashloan_call (mutable ?)
# 3. ethereum.transactions
# 6. sql.execute or self.execute?


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