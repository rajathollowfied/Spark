import requests,duckdb,json

url = "https://pokeapi.co/api/v2/pokemon/luxray/"

response = requests.get(url).json()
response = json.dumps(response)

query = "CREATE TABLE pokemon AS \
    SELECT * FROM read_json_auto(?)"
    
table = duckdb.execute(query, [response])

print(table)
