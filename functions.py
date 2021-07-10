import json
import requests
import time


def get_abi(address, schema, contract_name):
  # Check to see if the address has a corresponding ABI
  try: 
    with open(f"conf/{schema}/{contract_name}.abi") as f:
      abi = json.load(f)

  # If not, get it.
  except: 
      response = 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address=' + address + '&apikey=M36N6D99NY4U4E1GEIYYFYIERRR1MF5S8F'
      abi = json.loads(requests.get(response).json()['result'][0]['ABI'])
      contract_name = json.loads(requests.get(response).text)['result'][0]['ContractName'] # Get new contract name

  # Check if it's either a proxy address or implementation address.
  response = 'https://api.etherscan.io/api?module=contract&action=getsourcecode&address=' + address + '&apikey=M36N6D99NY4U4E1GEIYYFYIERRR1MF5S8F'
  source_code = requests.get(response).json()['result'][0]['SourceCode']
  #print(source_code)
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
      contract_name = json.loads(requests.get(response).text)['result'][0]['ContractName'] # Get new contract name

    except:
      print("This is an implementation contract. Implementation_slot in source code, but implementation contract not found")
      pass
  
  else:
    print("This is not an implementation/proxy contract")

  return address, abi, contract_name
# Feature? Permanently save the proxy contract and address combination to SQL. Read from there first before doing the above function.
