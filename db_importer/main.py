import json
from py2neo import authenticate, Graph, Node, Relationship

from constraints import define_data_constraints
from timeline_data import import_timeline_data
from order_rels import *
from product_defs import *

# MATCH (n:ORDER) WHERE NOT (n)-[:CONTAINS]->(:PRODUCT) RETURN n

def main():
    with open('../config.json') as config_data:
        config = json.load(config_data)
        [str(x) for x in config]
        host = config['host']

        authenticate(host['address'] + ':' + str(host['port']), host['username'], host['password'])
        graph = Graph(str(host['address']) + ':' + str(host['port']) + str(host['data_path']))

        define_data_constraints(graph)

        import_timeline_data(graph)

        import_order_product_rels(graph)
        import_order_user_rels(graph)

        import_cats(graph)
        import_tags(graph)

main()
