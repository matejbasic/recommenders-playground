import csv
import datetime
from py2neo import Node, Relationship
from helpers import *
from project_globals import *

DATASET_DIR = get_dataset_dir()
BATCH_SIZE  = get_batch_size()

def create_cats(graph):
    rows_total_c = 0
    dismissed_rels_c = 0
    bad_data_type_rows_c = 0
    cat_ids = []

    with open(DATASET_DIR + 'product-cat.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        batch = graph.begin()
        node_c = 0
        print 'CATEGORIES CREATED'
        for row in reader:
            rows_total_c += 1

            if rows_total_c % 1000 == 100:
                batch.commit()
                print 'BATCH COMMITED ' + str(node_c)
                print 'bad data type rows: ' + str(bad_data_type_rows_c) + ', total rows: ' + str(rows_total_c)
                batch = graph.begin()

            if is_number(row[2]):
                cat_id = int(row[2])
                if cat_id not in cat_ids:
                    cat_ids.append(cat_id)
                    batch.create(Node('CAT', oid=cat_id))
                    node_c += 1
            else:
                bad_data_type_rows_c += 1

        batch.commit()
        print 'BATCH COMMITED ' + str(node_c)
        print 'bad data type rows: ' + str(bad_data_type_rows_c) + ', total rows: ' + str(rows_total_c)

def create_cat_parent_relationships(graph):
    rows_total_c = 0
    bad_data_type_rows_c = 0
    dismissed_rels_c = 0
    with open(DATASET_DIR + 'product-cat.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        batch = graph.begin()
        rel_c = 0
        print 'CATEGORY PARENT RELATIONSHIPS'
        for row in reader:
            rows_total_c += 1
            if rows_total_c % 1000 == 100:
                batch.commit()
                print 'BATCH COMMITED ' + str(rel_c)
                print 'dismissed rows: ' + str(dismissed_rels_c) + ', total rows: ' + str(rows_total_c)
                batch = graph.begin()

            if is_number(row[2]) and is_number(row[3]):
                cc_node = graph.run('MATCH (cc:CAT) WHERE toInt(cc.oid)={x} RETURN cc', x=int(row[2])).evaluate()
                cp_node = graph.run('MATCH (cp:CAT) WHERE toInt(cp.oid)={x} RETURN cp', x=int(row[3])).evaluate()
                if cc_node is None or cp_node is None:
                    dismissed_rels_c += 1
                else:
                    rel_c += 1
                    batch.create(Relationship(cc_node, 'CHILD_OF', cp_node))
            else:
                bad_data_type_rows_c += 1

        batch.commit()
        print 'total rows: ' + str(rows_total_c) + ', missing parent: ' + str(dismissed_rels_c) + ', bad data type rows: ' + str(bad_data_type_rows_c)
        print 'total relationships: ' + str(rel_c)

def create_cat_product_relationships(graph):
    rows_total_c = 0
    bad_data_type_rows_c = 0
    dismissed_rels_c = 0
    with open(DATASET_DIR + 'product-cat.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        batch = graph.begin()
        rel_c = 0
        print 'CATEGORY PRODUCT RELATIONSHIPS'
        for row in reader:
            rows_total_c += 1
            if rows_total_c % 1000 == 100:
                batch.commit()
                print 'BATCH COMMITED ' + str(rel_c)
                print 'dismissed rows: ' + str(dismissed_rels_c) + ', total rows: ' + str(rows_total_c)
                batch = graph.begin()

            if is_number(row[0]) and is_number(row[2]):
                p_node = graph.run('MATCH (p:PRODUCT) WHERE toInt(p.oid)={x} RETURN p', x=int(row[0])).evaluate()
                c_node = graph.run('MATCH (c:CAT) WHERE toInt(c.oid)={x} RETURN c', x=int(row[2])).evaluate()
                if c_node is None or p_node is None:
                    dismissed_rels_c += 1
                else:
                    rel_c += 1
                    batch.create(Relationship(p_node, 'DEFINED', c_node))
            else:
                bad_data_type_rows_c += 1

        batch.commit()
        print 'total rows: ' + str(rows_total_c) + ', missing node/s: ' + str(dismissed_rels_c) + ', bad data type rows: ' + str(bad_data_type_rows_c)
        print 'total relationships: ' + str(rel_c)

def import_cats(graph):
    # Data structure: product_id,product_type,cat_id,parent_id,cat_name
    create_cats(graph)
    create_cat_parent_relationships(graph)
    create_cat_product_relationships(graph)

def create_tags(graph):
    rows_total_c = 0
    dismissed_rels_c = 0
    bad_data_type_rows_c = 0
    tag_ids = []

    with open(DATASET_DIR + 'product-tag.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        batch = graph.begin()
        node_c = 0
        print 'TAGS CREATED'
        for row in reader:
            rows_total_c += 1

            if rows_total_c % 1000 == 100:
                batch.commit()
                print 'BATCH COMMITED ' + str(node_c)
                print 'bad data type rows: ' + str(bad_data_type_rows_c) + ', total rows: ' + str(rows_total_c)
                batch = graph.begin()

            if is_number(row[1]):
                tag_id = int(row[1])
                if tag_id not in tag_ids:
                    tag_ids.append(tag_id)
                    batch.create(Node('TAG', oid=tag_id))
                    node_c += 1
            else:
                bad_data_type_rows_c += 1

        batch.commit()
        print 'BATCH COMMITED ' + str(node_c)
        print 'bad data type rows: ' + str(bad_data_type_rows_c) + ', total rows: ' + str(rows_total_c)

def create_tag_product_relationships(graph):
    rows_total_c = 0
    bad_data_type_rows_c = 0
    dismissed_rels_c = 0
    with open(DATASET_DIR + 'product-tag.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        batch = graph.begin()
        rel_c = 0
        print 'TAG PRODUCT RELATIONSHIPS'
        for row in reader:
            rows_total_c += 1
            if rows_total_c % 1000 == 100:
                batch.commit()
                print 'BATCH COMMITED ' + str(rel_c)
                print 'dismissed rows: ' + str(dismissed_rels_c) + ', total rows: ' + str(rows_total_c)
                batch = graph.begin()

            if is_number(row[0]) and is_number(row[1]):
                p_node = graph.run('MATCH (p:PRODUCT) WHERE toInt(p.oid)={x} RETURN p', x=int(row[0])).evaluate()
                t_node = graph.run('MATCH (t:TAG) WHERE toInt(t.oid)={x} RETURN t', x=int(row[1])).evaluate()
                if t_node is None or p_node is None:
                    dismissed_rels_c += 1
                else:
                    rel_c += 1
                    batch.create(Relationship(p_node, 'DEFINED', t_node))
            else:
                bad_data_type_rows_c += 1

        batch.commit()
        print 'total rows: ' + str(rows_total_c) + ', missing node/s: ' + str(dismissed_rels_c) + ', bad data type rows: ' + str(bad_data_type_rows_c)
        print 'total relationships: ' + str(rel_c)

def import_tags(graph):
    # Data structure: product_id,tag_id,tag_name
    create_tags(graph)
    create_tag_product_relationships(graph)
