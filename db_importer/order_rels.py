import csv
import datetime
from py2neo import Node, Relationship
from helpers import *
from project_globals import *

DATASET_DIR = get_dataset_dir()
BATCH_SIZE  = get_batch_size()

def import_order_product_rels(graph):
    # Data structure: order_id,product_id,product_quantity,product_total
    print 'ORDER -CREATE-> PRODUCT RELS'
    with open(DATASET_DIR + 'order-product.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        rel_c = 0
        bad_row_c = 0
        none_p_nodes_c = 0
        none_o_nodes_c = 0
        none_all_nodes_c = 0
        rels = []
        for row in reader:
            # check the data fields type
            # refct func as arg
            if row[0].isdigit() and row[2].isdigit() and row[2] > 0 and is_number(row[3]):
                o_node = graph.run('MATCH (o:ORDER) WHERE o.oid={x} RETURN o', x=int(row[0])).evaluate()
                p_node = graph.run('MATCH (p:PRODUCT) WHERE p.oid={x} RETURN p', x=int(row[1])).evaluate()

                if p_node is None:
                    none_p_nodes_c += 1
                if o_node is None:
                    none_o_nodes_c += 1

                if p_node is None or o_node is None:
                    none_all_nodes_c += 1
                else:
                    unit_price = float(row[3]) / int(row[2])
                    rels.append(Relationship(o_node,'CONTAINS',p_node, quantity=int(row[2]), total=float(row[3]), unit_price=unit_price))

            else:
                bad_row_c += 1

    batch = graph.begin()
    for rel in rels:
        rel_c += 1
        batch.create(rel)

        if rel_c == BATCH_SIZE:
            batch.commit()
            print 'BATCH COMMITED ' + str(rel_c) + ', bad data type rows: ' + str(bad_row_c)
            batch = graph.begin()

    batch.commit()
    print 'BATCH COMMITED ' + str(rel_c) + ', bad data type rows: ' + str(bad_row_c)

    print 'missing P or O: ' + str(none_all_nodes_c) + ', missing P: ' + str(none_o_nodes_c) + ', missing O: ' + str(none_p_nodes_c)

def import_order_user_rels(graph):
    # Data structure: user_id,order_id,order_date
    print 'USER -PURCHASED-> ORDER RELS'
    none_o_nodes_c = 0
    none_u_nodes_c = 0
    rels = []
    with open(DATASET_DIR + 'user-order.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            # print row
            u_node = graph.run('MATCH (u:USER) WHERE u.oid={x} RETURN u', x=int(row[0])).evaluate()
            o_node = graph.run('MATCH (o:ORDER) WHERE o.oid={x} AND (o)-[:CONTAINS]->() RETURN o', x=int(row[1])).evaluate()
            if o_node is None:
                none_o_nodes_c += 1
            elif u_node is None:
                none_u_nodes_c += 1
            else:
                rels.append(Relationship(u_node,'PURCHASED',o_node))

    rels_counter = 0
    batch = graph.begin()
    for rel in rels:
        rels_counter += 1
        batch.create(rel)

        if rels_counter == BATCH_SIZE:
            batch.commit()
            batch = graph.begin()
            print 'BATCH COMMITED ' + str(rels_counter)

    batch.commit()
    print 'BATCH COMMITED ' + str(rels_counter) + ' missing O: ' + str(none_o_nodes_c) + ' missing P: ' + str(none_u_nodes_c)
