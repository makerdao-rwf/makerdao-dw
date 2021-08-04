from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy import text
import time

from functions import get_conf, get_schema_and_contract

# Get user input - schema, contract name, and database information (conf file)
schema, contract_name = get_schema_and_contract()
conf = get_conf()

    
class SqlEngine:
  '''Retrieves latest blocks and creates db tables if none exist'''

  def __init__(self, abi):
    self.abi = abi

  # engine var,common_columns, and type_mapping should be defined by the subclasses otherwise it will crash. 
  # What is meant by this comment? These are always defined. If engine cannot be initialized, it should crash.

  # Connect to the sqlalchemy engine
  def connect(self):
    return self.engine.connect()

  ## Create a sessionmaker object and run engine.begin().
  def begin(self):
    return sessionmaker(self.engine).begin()

  ## Execute a transaction
  def execute(self, sql):
    return self.engine.execute(sql)

  def get_latest_block(self, fromBlock):
    print('Connected to', self.engine)
    '''find the last blocknumber in the database for this contract. NOTE: DO with eth-blocks afterwards'''
    with self.connect(): 
      for j in self.abi:
        if (j["type"] == "function" and j["stateMutability"] != "view") or (j["type"] == "event" and j["anonymous"] != True):
          table_name = j['table']
          sql_check_table_block = f"""select max(block_number) from {self.db}.{schema}."{table_name}" """

          try: # Added this try/except. If there isn't a table, then just start at the beginning. 
            max_block = self.execute(text(sql_check_table_block)).scalar()
            if max_block != None and max_block >= fromBlock:
              fromBlock = max_block + 1
          except:
            fromBlock = conf["contracts"][schema][contract_name]["creationBlock"]
      
      return fromBlock

  def create_schema (self):
    with self.connect() as sql: # NOTE: ONLY DO THIS ONCE ?
      self.execute(text(f"create schema if not exists {schema}")) #Does this need to be here? I don't think it does...
      for j in self.abi:
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
                #print("sql columns:", columns)
              except KeyError:
                print("There is probably an unsupported datatype. You can add more to the type_mapping dict above")
                raise
            sql_create_table = f"""create table if not exists {schema}."{table_name}" ( {columns} )""" 
            print(sql_create_table)
            self.execute(text(sql_create_table))
          else:
            print('Tables already exist')
            break


class SnowflakeEngine(SqlEngine):
  def __init__ (self, abi, user, password, account, db):
    super().__init__(abi)
    self.engine = create_engine(f'snowflake://{user}:{password}@{account}/{db}')
    self.db = db
    self.common_columns = "block_number bigint, block_hash string, address string, log_index int, transaction_index int, transaction_hash string"
    self.type_mapping = {"address": "string", "bytes": "string", "bytes4": "string", "bytes32": "string", "int256": "numeric", "uint256": "string", "uint16":"numeric", "bool": "boolean", "address[]":"string", "uint256[]":"string", "uint8":"numeric", "string":"string"} #NOTE: I changed uint256 and uint256[] to string...

  def encode_functions(self, j, params, values):
    '''Encode function parameters for Snowflake'''   
    for idx, value in enumerate(params):
      if isinstance(value, bytes):
        values += ", '" + value.hex() + "'"
      else:
        values += ", " + "'" + str(value)+ "'"

    self.values = values

  def encode_events(self, j, event_data, values):
    '''Encode event parameters for Snowflake'''
    for idx, event_param in enumerate(event_data["data"]):
      value = event_param["value"]
      if isinstance(value, bytes):
        values += ", '" + value.hex() + "'"
      else:
        values += ", " + "'" + str(value)+ "'"

    self.values = values

  def insert(self, t, table_name, session):
    '''Insert values into Snowflake'''
    values = f"{t.blockNumber}, '{t.blockHash.hex()}', '{t.address}', {t.logIndex}, {t.transactionIndex}, '{t.transactionHash.hex()}' {self.values}"
    sql_insert = f"""insert into {schema}."{table_name}" values ({values})"""
    print(text(sql_insert))

    #start = time.time()
    session.execute(text(sql_insert)) #There might be a faster way? self.execute() is 2-3x slower.
    #end = time.time()
    #print("TIME insert:", end - start)



class PostgresqlEngine(SqlEngine):
  def __init__ (self, abi, host, user, password, db, port):
    super().__init__(abi)
    self.engine = create_engine(f'postgresql://{user}:{password}@{host}{port}{db}') #echo=True to show all sql queries
    self.db = db
    self.common_columns = "block_number bigint, block_hash bytea, address bytea, log_index int, transaction_index int, transaction_hash bytea"
    self.type_mapping = {"address": "bytea", "bytes": "bytea", "bytes4": "bytea", "bytes32": "bytea", "int256": "numeric", "uint256": "numeric", "uint16":"numeric", "bool": "boolean", "address[]":"bytea", "uint256[]":"numeric", "uint8":"numeric", "string":"bytea"}

  def encode_functions(self, j, params, values):
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
    self.values = values

  def encode_events(self, j, event_data, values):
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
    self.values = values

  def insert(self, t, table_name, session):
    '''Insert values into Postgresql'''
    values = f"{t.blockNumber}, '\\{t.blockHash.hex()[1:]}', '\\{t.address[1:]}', {t.logIndex}, {t.transactionIndex}, '\\{t.transactionHash.hex()[1:]}' {self.values}"
    sql_insert = f"""insert into {schema}."{table_name}" values ({values})"""
    print(text(sql_insert))
    session.execute(text(sql_insert))




def start_engine(abi, db_driver, db_host, db_user, db_password, db_account, db_db, db_port):
  ''' A function to initialize either the SnowflakeEngine class or PostgresqlEngine class '''
  if db_driver == "snowflake":
    return SnowflakeEngine(abi, db_user, db_password, db_account, db_db)
  elif db_driver == "postgresql":
    return PostgresqlEngine(abi, db_host, db_user, db_password, db_db, db_port)
  else:
    print("Please verify that your driver is named either 'snowflake' or 'postgresql' in template.conf")
    return None