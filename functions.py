import json
import requests
import time
import snowflake.connector as sf
import eth_event


def get_abi(address, schema, contract_name):
  # Check to see if the address has a corresponding ABI
  try: 
    with open(f"conf/{schema}/{contract_name}.abi") as f:
      abi = json.load(f)

  # If not, get it.
  except: 
      response = 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address=' + address + '&apikey=M36N6D99NY4U4E1GEIYYFYIERRR1MF5S8F'
      abi = json.loads(requests.get(response).json()['result'][0]['ABI'])
      #contract_name = json.loads(requests.get(response).text)['result'][0]['ContractName'] # Get new contract name
      print("Retrieved the ABI")

  return address, abi
'''
# This code will retrieve the proxy address, but it's slow right now and possibly not necessary.

  # Check if it's either a proxy address or implementation address.
  response = 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address=' + address + '&apikey=M36N6D99NY4U4E1GEIYYFYIERRR1MF5S8F'
  source_code = requests.get(response).json()['result'][0]['SourceCode']

  if 'IMPLEMENTATION_SLOT =' in source_code: 
    print("This is a proxy/implementation contract")
    try: # If so, grab and replace the ABI, address, and contract name
      params = (('module', 'contract'),('action', 'verifyproxycontract'),('apikey', 'M36N6D99NY4U4E1GEIYYFYIERRR1MF5S8F'),) #Your Etherscan API key
      data = {'address': address}
      response = requests.post('https://api.etherscan.io/api', params=params, data=data).json()
      result = response['result'] 
      time.sleep(7) # We need to wait between API calls on Etherscan or else it'll throw an error message

      # Second API call. Use 'guid' to retrieve proxy contract address
      params2 = (('module', 'contract'),('action', 'checkproxyverification'),('guid', result),('apikey', 'M36N6D99NY4U4E1GEIYYFYIERRR1MF5S8F'),)
      response = requests.get('https://api.etherscan.io/api', params=params2)
      address = response.text.split("contract is found at ",1)[1].split(" and is ")[0] #extract proxy address from response
      print("This is a proxy contract. An implementation contract and ABI were found")
      time.sleep(6)
      
      # Get new ABI and contract name
      response = 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address=' + address + '&apikey=M36N6D99NY4U4E1GEIYYFYIERRR1MF5S8F'
      print(response)
      abi = json.loads(requests.get(response).json()['result'][0]['ABI'])
      #contract_name = json.loads(requests.get(response).text)['result'][0]['ContractName'] # Get new contract name

    except:
      print("This might be an implementation contract. Implementation_slot in source code, but implementation contract not found")
      pass
  
  else:
    print("This is not an implementation/proxy contract")
  
  return address, abi
  '''
# Feature? Permanently save the proxy contract and address combination to SQL. Read from there first before doing the above function.



# add tablename to abi json 
dict_fn = {} ## Manage an index for disambuguation of functions with same names but different signature
dict_evt = {} ## Manage an index for disambuguation of events  with same names but different signature
dict_sign = {} # function /events signature to item (function or event) abi

def get_abi_params(abi, contract_name, w3):
  for j in abi:
    if j["type"] == "function" and j["stateMutability"] != "view":
      fn_name = j["name"].lower() 
      signature = '{}({})'.format(j['name'],','.join([input['type'] for input in j['inputs']]))
      #print(j['name'],','.join([input['type'] for input in j['inputs']]))
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
  return j, dict_evt, dict_fn, dict_sign


'''
def to_snowflake(): #Give new names

  # Connect to Snowflake

  conn = sf.connect(
  user= snowflake_user, #userid
  password="1Helse123", #password
  account="KL63833.west-us-2.azure", #organization_name.account_name
  )

  # Creating a Database, Schema, and Warehouse
  # conn.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS maker_warehouse_mg")
  # conn.cursor().execute("CREATE DATABASE IF NOT EXISTS testdb_mg")
  # conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS testschema_mg")

  # Set the Database, Schema, and Warehouse
  conn.cursor().execute("USE WAREHOUSE maker_warehouse")
  conn.cursor().execute("USE DATABASE testdb_mg")
  conn.cursor().execute("USE SCHEMA testdb_mg.testschema_mg") 

  # Creating Tables and Inserting Data
  conn.cursor().execute(
  "CREATE OR REPLACE TABLE "
  "test_table(col1 integer, col2 string)" 
  )

  # Insert
  conn.cursor().execute("INSERT INTO test_table(col1, col2) VALUES (%s, %s)", ('123', 'indian Cricket'))

  return 
'''