import json
import pandas as pd

if (__name__ == '__main__'):
    with open('config.json') as config_file:
        data = json.load(config_file)

    data_nodes = data['DataNodes']
    replica_factor = data['Rfactor']
    #print(data_nodes[0]['IPv4'])
    for node in data_nodes:
        print(node)
    print(replica_factor)

    LOOKUP_TABLE = pd.DataFrame()

