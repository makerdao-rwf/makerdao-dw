from web3 import Web3
import time
from sqlalchemy import text
from datetime import datetime


from classes import start_engine
from functions import get_conf


# Get Parameters
conf = get_conf()
infura_key = conf["infura_key"]

db_driver = conf["db.driver"] # snowflake or postgresql
db_host = conf["db.host"]
db_user = conf["db.user"]
db_password = conf["db.password"]
db_db = conf["db.database"]
db_port = conf["db.port"] 
db_account = conf["db.account"]
db_warehouse = conf["db.warehouse"]

schema = "ethereum"
table_name = "transactions"
creationBlock = conf["contracts"][schema][table_name]["creationBlock"]
abi = [{"inputs":[{"internalType":"address","name":"transaction","type":"address"}],"name":"transactions","outputs":[],"stateMutability":"nonpayable","type":"function","table":"transactions"}] #Defined ABI with a fake list here so that get_latest_block will run correctly


# Create SQL Alchemy Engine
try:
  engine = start_engine(abi, db_driver, db_host, db_user, db_password, db_account, db_db, db_port)
except:
  print("The postgresql/snowflake engine could not be initialized. Verify that template.conf is setup correctly. There should be no empty fields.")
  raise ValueError()


# Set w3 source to Infura Mainnet
w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/' + infura_key)) 


# Get latest block number and create schema
fromBlock = engine.get_latest_block(creationBlock)

# Initialize unique column datatypes for postgresql and snowflake
if db_driver == 'postgresql':
  columns = "block_number bigint, block_hash bytea, miner bytea, nonce bytea, gas_limit bigint, gas_used bigint, difficulty bigint, extra_data bytea, time timestamp, size bigint"
  type_mapping = {"address": "bytea", "bytes": "bytea", "bytes4": "bytea", "bytes32": "bytea", "int256": "numeric", "uint256": "numeric"}
elif db_driver == 'snowflake':
  columns = "block_number bigint, block_hash string, miner string, nonce string, gas_limit bigint, gas_used bigint, difficulty bigint, extra_data string, time timestamp, size bigint"
  type_mapping = {"address": "string", "bytes": "string", "bytes4": "string", "bytes32": "string", "int256": "numeric", "uint256": "string", "uint16":"numeric", "bool": "boolean", "address[]":"string", "uint256[]":"string", "uint8":"numeric", "string":"string"} #NOTE: I changed uint256 and uint256[] to string...
  
# Create schema and table if they don't exist already
with engine.connect() as sql:
    sql_create_table = f"""create table if not exists {schema}."{table_name}" ( {columns} )"""
    sql_create_schema = f"""create schema if not exists {schema}"""
    sql.execute(text(sql_create_schema))
    sql.execute(text(sql_create_table))


print(f"Start from block {fromBlock}")

# Init column types and insert values into table
while fromBlock < w3.eth.block_number:
  with engine.begin() as session: 
    block = w3.eth.get_block(fromBlock, full_transactions = False)
    block_number = block["number"]
    block_hash = block["hash"] # HexBytes
    miner = block["miner"] # Address as text
    nonce = block["nonce"] # HexBytes
    gas_limit = block["gasLimit"]
    gas_used = block["gasUsed"]
    difficulty = block["difficulty"]
    extra_data = block["extraData"] # HexBytes
    time = datetime.fromtimestamp(block["timestamp"])
    size = block["size"]

    # Encode input values differently for postgresql and snowflake
    if db_driver == "postgresql":
      sql_insert = f"""insert into {schema}."{table_name}" values ({block_number}, '\\{block_hash.hex()[1:]}', '\\{miner[1:]}', '\\{nonce.hex()[1:]}', {gas_limit}, {gas_used}, {difficulty}, '\\{extra_data.hex()[1:]}', '{time}', {size})"""
    elif db_driver == "snowflake":
      sql_insert = f"""insert into {schema}."{table_name}" values ({block_number}, '{block_hash.hex()}', '{miner}', '{nonce.hex()}', {gas_limit}, {gas_used}, {difficulty}, '{extra_data.hex()}', '{time}', {size})"""
    
    # Insert values
    session.execute(text(sql_insert))
    print(text(sql_insert))

    fromBlock += 1


# NOTE: This is not batch uploading; each transaction is uploaded one at a time. Should we implement blockstep to speed this up?