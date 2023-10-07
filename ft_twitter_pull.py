import requests
import pandas as pd
import web3
import os
import json
from dotenv import load_dotenv
from tqdm import tqdm
from time import sleep

#py -m venv venv
#copy venv->scypts->activate->copy relative path-> paste into terminal

# Step 1: Pull account data from Dune
# Step 2: Use dune data to pull FT account data from ft api
# Step 3: Push data to Dune
load_dotenv()


# Request data from FT api
def getInfoFromAddress(address):
    # returns user info from an address
    url = f'https://prod-api.kosetto.com/users/{address}'
    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \ (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    response = requests.get(url, headers=headers)
    return response

## Use Dune to pull latest FT account data
def executeQuery(query_id, api_key, perf="large"):
    # get API key
      # Slice FT ALL Traders API
    # authentiction with api key
    headers = {"X-Dune-API-Key": api_key}
    base_url = f"https://api.dune.com/api/v1/query/{query_id}/execute"
    params = {"performance": perf}
    result_response = requests.request("POST", base_url, headers=headers, params=params)

    results = result_response.json()
    print(results)
    execution_id = results['execution_id']
    state = results['state']
    status_url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"

    while state == "QUERY_STATE_PENDING" or state == "QUERY_STATE_EXECUTING":
        resp = requests.get(status_url, headers=headers)
        state = resp.json()["state"]
        print(f'Query State: {state}')
        sleep(2)

    results_url = f"https://api.dune.com/api/v1/query/{query_id}/results"
    resp = requests.get(results_url, headers=headers)

    return resp

# Upload data to dune
def duneUpload(csv_file_path, api_key, table_name):
    url = 'https://api.dune.com/api/v1/table/upload/csv'

    with open(csv_file_path) as open_file:
        data = open_file.read()
        
        headers = {'X-Dune-Api-Key': api_key}

        payload = {
            "table_name": table_name,
            "description": "blank",
            "data": str(data)
        }
        
        response = requests.post(url, data=json.dumps(payload), headers=headers)

        print('Response status code:', response.status_code)
        print('Response content:', response.content)

#Execute Dune query
api_key = os.environ.get("api_key")
qid = 3085271  
results = executeQuery(qid,api_key)

#Format dune data
final_data = results.json()['result']['rows'] 
trader_values = [item['trader'] for item in final_data]

#Format address
checkSum = []
for j in trader_values:
    addressCheckSum = web3.Web3.to_checksum_address(j)
    checkSum.append(addressCheckSum)
print(checkSum[:5])


# check known values
df = pd.read_csv('ft_user_addresses_raw.csv')
address_list = df['Trader_Address'].values.tolist()  #get me this column - .values says not header - .tolist says conver to list

#compare checksum(dune data) to know addres values
new_address = []
for values in checkSum: 
    if values not in address_list:
        new_address.append(values)
    

#outer loop - Create batches e.g. (0,500,1000,1500) and then i will iterate through those values
batch_size = 500  # Number of records to fetch before saving


for i in tqdm(range(0, len(new_address), batch_size)): 
#for i in tqdm(range(0, 2)):
    batch_data = []
    address_data = []
    start_index = i
    
    if len(new_address) > start_index + batch_size:
        end_index = start_index+batch_size
    else:
        end_index = len(new_address)

    try:     
        # Fetch data for each address in the batch
        # Here we pass i into inner loop so that it runs this loop in batches (run first 0+500, then 500:1000, etc...)
        for address in tqdm(new_address[start_index:end_index]):
            #1250
            try:
                data = getInfoFromAddress(address)
                batch_data.append(data.json().get('twitterUsername'))
                address_data.append(address)
                #sleep(0.1)  # random guess to prevent rate limits
            except Exception as e:
                print(f"\nError for {address}: {e}") #f" reques {} to pass in variables
                batch_data.append(None) #just append none (is none similar to null?)
                address_data.append(address)
    finally:
    
    # Save the batch data to CSV
        print(len(address_data))
        print(len(batch_data))
        temp_df = pd.DataFrame({'Trader_Address': address_data, 'Trader_Twitter': batch_data})
        print(temp_df)
        df = pd.concat([df,temp_df])
        print(len(df))
        df.to_csv('ft_user_addresses_raw.csv', index=False)
        #temp_df.to_csv('ft_user_addresses_raw.csv', index=False)


# #Pull twitter data from ft api ----------------------------------------
# data_list = []

# for i in tqdm(checkSum):
#     data = getInfoFromAddress(i)
#     data_list.append(data.json().get('twitterUsername'))
#     sleep(0.5)
# print(data_list[:5])

# #Join address and api data and store in csv
# final_df = pd.DataFrame({'Trader_Address':checkSum, 'Trader_Twitter':data_list}) 
# final_df.to_csv('ft_user_addresses_raw.csv',index=False)

#------------------------

api_key = os.environ.get("dune_api")
csv_file_path = 'ft_user_addresses_raw.csv'
table_name = 'ft_twitter_data1'

response = duneUpload(csv_file_path,api_key,table_name)
# print(response)










# References
#---------------------------------------

#df = pd.DataFrame(results.json()['result']['rows'])
# Look into: pd.json_nomralize

# Write to CSV
#df.to_csv('ft_user_addresses_raw.csv', header = None, index=False)

# Read CSV without header
# df = pd.read_csv('file_path.csv', header=None)