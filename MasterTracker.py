import json

with open('config.json') as config_file:
    data = json.load(config_file)

data_nodes = data['DataNodes']
replica_factor = data['Rfactor']
print(data_nodes[0]['IPv4'])
for node in data_nodes:
    print(node)
print(replica_factor)
