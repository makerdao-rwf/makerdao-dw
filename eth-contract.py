from sqlalchemy.sql.expression import table
from web3 import Web3
import eth_event
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
engine = start_engine(abi, db_driver, db_host, db_user, db_password, db_account, db_db, db_port)

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
  print(f"Fetching events from block {fromBlock} to {toBlock}", "\nBlockStep:", blocksStep)
  cnt = 0

  # Initialize sessionmaker object
  with engine.begin() as session: 
    for address in addresses:

      # Retrieve logs for each address
      try:
        for t in read_logs(address, fromBlock, toBlock): 
          try:
            # Construct j and table name based on the topic found in t (methodid)
            j = dict_sign[t.topics[0].hex()]
            table_name = j["table"] 
            values = ""
          except KeyError:
            pass

          #If the transaction's signature is a function, decode the log.
          if j["type"] == "function" and j["stateMutability"] != "view":
            try:
              inputs, params, methodid = get_function_data(t, contract) #this might be able to be sped up.
              #print("inputs:", inputs, "\n params:", params)
            except:
              raise ValueError("The input data is not readable. You may be using the wrong ABI.")
          
            # Replace j if the input data contains the signature/methodid instead of topic 0 ('execute' transactions).
            if contract_name == 'proxy_actions':
              j = dict_sign[methodid]
              table_name = j["table"]
              values = ""

            # Encode function data for SQL
            try:
              engine.encode_functions(j, params, values)
            except:
              print('Could not encode parameters: \n','type1', type(params[0]), 'type2', type(params[1]), 'type3', type(params[2]))
              continue #CONTINUE IF IN LOOP. If it can't encode it, is it okay to write it as it is?

          # If the signature in t is an event, decode log and encode the result for SQL  
          elif j["type"] == "event" and j["anonymous"] != True:
            params = eth_event.decode_log(t, eth_event.get_topic_map(abi))
            engine.encode_events(j, params, values)
          else:
            continue
          
          # Insert values
          engine.insert(t, table_name, session) 
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
# 6. Is there a faster way to insert the data? It felt faster when it wasn't in a separate class (same file). Double check.
