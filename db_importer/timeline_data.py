import csv
import datetime
import time
from py2neo import Node, Relationship
from helpers import *
from project_globals import *

DATASET_DIR = get_dataset_dir()
BATCH_SIZE  = get_batch_size()

def get_data_rows(file_name, label):
    labeled_rows = []
    with open(DATASET_DIR + file_name, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            labeled_rows.append([label] + row)

    # for row in labeled_rows:
    #     print row
    return labeled_rows

def get_part_of_day(dt):
    if dt.hour >= 5 and dt.hour < 12:
        return 'Morning'
    elif dt.hour >= 12 and dt.hour < 17:
        return 'Afternoon'
    elif dt.hour >= 17 and dt.hour < 21:
        return 'Evening'
    else:
        return 'Night'

def get_timeline_rows():
    # merge all data with timestamp (date created)
    timeline_rows = []
    # Data structure: user_id,user_registered,users_country,users_state,users_city,has_purchased,order_count,paying_customer,money_spent
    timeline_rows = get_data_rows('user.csv', 'user')
    # Data structure: product_id,parent_id,product_date,product_type,product_price,product_min_var_price,product_max_var_price
    for row in get_data_rows('product.csv', 'product'):
        timeline_rows.append(row)
    # Data structure: user_id,order_id,order_date
    for row in get_data_rows('user-order.csv', 'user-order'):
        timeline_rows.append(row)
    return timeline_rows

def process_timeline_data():
    # merge all data with timestamp (date created)
    timeline_rows = get_timeline_rows()
    timeline_rows_processed = []

    for row in timeline_rows:
        if row[0].strip() == 'product' or row[0].strip() == 'user-order':
            row_timestamp = row[3]
        elif row[0].strip() == 'user':
            row_timestamp = row[2]

        dt = datetime.datetime.strptime(row_timestamp, '%Y-%m-%d %H:%M:%S')
        # print dt.strftime('%Y') + ' ' + dt.strftime('%m') + ' ' + dt.strftime('%A') + ' ' + str(dt.hour) + ' ' + get_part_of_day(dt)
        timeline_rows_processed.append([dt] + [dt.strftime('%Y')] + [dt.strftime('%m')] + [dt.strftime('%A')] + [dt.hour] + [get_part_of_day(dt)] + row)

    timeline_rows_processed.sort()
    return timeline_rows_processed

def import_timeline_data(graph):

    timeline_rows_processed = process_timeline_data()

    current_dt = None
    prev_tf_node = None
    # counters
    row_processed_c = 0
    tf_nodes_c = 0
    u_nodes_c = 0
    p_nodes_c = 0
    o_nodes_c = 0
    batch_c = 0

    # graph.delete_all()
    batch = graph.begin()

    for row in timeline_rows_processed:
        # dt = row[0].replace(second=0)
        dt = row[0]
        row_processed_c += 1
        batch_c += 1

        if current_dt != dt:
            tf_nodes_c += 1
            labels = []
            if current_dt is None:
                labels += ['FIRST_TIME_FRAME']
            elif row_processed_c == len(timeline_rows_processed):
                labels += ['LAST_TIME_FRAME']
            current_dt = dt
            # props
            tf_node = Node('TIME_FRAME',
            # define secs
                timestamp=current_dt.strftime('%Y-%m-%d %H:%M:%S'),
                year=int(row[1]),
                month=int(row[2]),
                day_in_week=str(row[3]),
                hour=int(row[4]),
                part_of_day=str(row[5])
            )
            # labels
            for frame_label in labels:
                tf_node.add_label(frame_label)
            #rels
            batch.create(tf_node)
            if prev_tf_node is not None:
                next_frame_rel = Relationship(prev_tf_node, 'NEXT_FRAME', tf_node)
                batch.create(next_frame_rel)

            prev_tf_node = tf_node

        if row[6] == 'user':
            u_nodes_c += 1
            # props
            if is_number(row[13]):
                order_count = int(row[13])
            else:
                order_count = 0

            if is_number(row[15]):
                money_spent = float(row[15])
            else:
                money_spent = 0

            city    = row[11].decode('latin-1').encode('utf-8')
            country = row[9].decode('latin-1').encode('utf-8')
            state   = row[10].decode('latin-1').encode('utf-8')

            u_node = Node('USER',
                oid=int(row[7]),
                country=country,
                state=state,
                city=city,
                order_count=order_count,
                money_spent=money_spent
            )
            # labels
            if order_count > 0:
                u_node.add_label('HAS_PURCHASED')
            # print u_node
            #rels
            batch.create(u_node)
            user_created_rel = Relationship(u_node, 'CREATED_AT', tf_node)
            batch.create(user_created_rel)
        elif row[6] == 'product':
            p_nodes_c += 1
            if is_number(row[11]):
                price = float(row[11])
            else:
                price = 0
            if is_number(row[12]):
                min_var_price = float(row[12])
            else:
                min_var_price = 0
            if is_number(row[13]):
                max_var_price = float(row[13])
            else:
                max_var_price = 0
            p_node = Node('PRODUCT',
                oid=int(row[7]),
                price=price,
                min_var_price=min_var_price,
                max_var_price=max_var_price
            )

            # print p_node
            #rels
            batch.create(p_node)
            product_created_rel = Relationship(p_node, 'CREATED_AT', tf_node)
            batch.create(product_created_rel)
        elif row[6] == 'user-order':
            # Data structure: user_id,order_id,order_date
            o_nodes_c += 1
            o_node = Node('ORDER', oid=int(row[8]))
            # print o_node
            #rels
            batch.create(o_node)
            order_created_rel = Relationship(o_node, 'CREATED_AT', tf_node)
            batch.create(order_created_rel)

        if batch_c == BATCH_SIZE:
            batch.commit()
            # time.sleep(14)
            print 'BATCH COMMITED TF: ' + str(tf_nodes_c) + ', U: ' + str(u_nodes_c) + ', P: ' + str(p_nodes_c) + ', O: ' + str(o_nodes_c)
            batch_c = 0
            batch = graph.begin()

    batch.commit()
    print 'BATCH COMMITED TF: ' + str(tf_nodes_c) + ', U: ' + str(u_nodes_c) + ', P: ' + str(p_nodes_c) + ', O: ' + str(o_nodes_c)

    print 'PROCESSED rows: ' + str(row_processed_c) + ', TF: ' + str(tf_nodes_c) + ', U: ' + str(u_nodes_c) + ', P: ' + str(p_nodes_c) + ', O: ' + str(o_nodes_c)
