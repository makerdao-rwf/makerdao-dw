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
from datetime import datetime


import argparse

parser = argparse.ArgumentParser(description='Parse and store Ethereum blocks on a database.')


conf = ConfigFactory.parse_file('config.conf')

## Environment parameters
infura_key = conf["infura_key"]

## Input parameters
db_host = conf["db.host"]
db_user = conf["db.user"]
db_password = conf["db.password"]
db_db = conf["db.database"]


schema = "ethereum"
table_name = "blocks"

fromBlock = 0 # We start from block 0 if the database is empty

# Connect to PostgreSQL
engine = create_engine('postgresql://'+db_user+':'+db_password+'@'+db_host+':5432/'+db_db) 



# Set w3 source to Infura Mainnet
w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/' + infura_key)) 


# create all tables if needed
columns = "block_number bigint, block_hash bytea, miner bytea, nonce bytea, gas_limit bigint, gas_used bigint, difficulty bigint, extra_data bytea, time timestamp, size bigint"

type_mapping = {"address": "bytea", "bytes": "bytea", "bytes4": "bytea", "bytes32": "bytea", "int256": "numeric", "uint256": "numeric"}

with engine.connect() as sql:
  sql_check_table_exists = f"select count(*) from information_schema.tables where table_schema = '{schema}' and table_name = '{table_name}'"
  if sql.execute(text(sql_check_table_exists)).scalar() == 0:
    sql_create_table = f"""create table {schema}."{table_name}" ( {columns} )"""
    print(sql_create_table)
    sql.execute(text(f"create schema if not exists {schema}"))
    sql.execute(text(sql_create_table))


# find the last blocknumber in the database for this contract
with engine.connect() as sql:
  max_block = sql.execute(text(f"""select max(block_number) from {schema}."{table_name}" """)).scalar()
  if max_block != None and max_block > fromBlock:
    fromBlock = max_block + 1

print(f"Start from block {fromBlock}")


lastBlock = w3.eth.block_number



while fromBlock < lastBlock:
  with sessionmaker(engine).begin() as session: 
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

    session.execute(text(f"""insert into {schema}."{table_name}" values ({block_number}, '\\{block_hash.hex()[1:]}', '\\{miner[1:]}', '\\{nonce.hex()[1:]}', {gas_limit}, {gas_used}, {difficulty}, '\\{extra_data.hex()[1:]}', '{time}', {size})"""))
    fromBlock += 1
