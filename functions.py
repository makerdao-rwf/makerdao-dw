import json
import requests
import eth_event
import argparse
from pyhocon import ConfigFactory

def get_schema_and_contract():
  parser = argparse.ArgumentParser(description='Parse a contract on the Ethereum blockchain and store logs on a database.')
  parser.add_argument('contract', help='name of the contract to parse like makermcd.vat (<schema>.<contract>)')
  args = parser.parse_args()

  schema, contract_name = args.contract.split(".")
  print(f"Parsing contract {schema}.{contract_name}")
  return schema, contract_name

def get_conf():
  conf = ConfigFactory.parse_file('config.conf')
  return conf

def get_abi(address, schema, contract_name):
  # Check to see if the address has a corresponding ABI
  try: 
    with open(f"conf/{schema}/{contract_name}.abi") as f:
      abi = json.load(f)

  # If not, get it.
  except: 
      response = 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address=' + address + '&apikey=M36N6D99NY4U4E1GEIYYFYIERRR1MF5S8F'
      abi = json.loads(requests.get(response).json()['result'][0]['ABI'])

      abi_file = open(f"conf/{schema}/{contract_name}.abi",'w')
      abi_file.write(str(abi))
      abi_file.close()

      print("Retrieved and saved the ABI")

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
# Permanently save the proxy contract abi to the .abi file



# add tablename to abi json 
dict_fn = {} ## Manage an index for disambuguation of functions with same names but different signature
dict_evt = {} ## Manage an index for disambuguation of events  with same names but different signature
dict_sign = {} # function /events signature to item (function or event) abi

def get_abi_params(abi, contract_name, w3):
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
  return j, dict_evt, dict_fn, dict_sign



def get_function_data(t, contract):
  '''Convert each event's input data to a readable format. Then decode it.'''
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
          raise ValueError

        pass

  methodid = input_data[:10] + '00000000000000000000000000000000000000000000000000000000'
  return inputs, params, methodid

  