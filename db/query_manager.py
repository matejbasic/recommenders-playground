import math
import json
from py2neo import authenticate, Graph

class QueryManager:

    def __init__(self, config_path = None, train_set_size = 0.7):
        self.config_path = config_path
        self.set_graph(config_path)
        self.set_test_size(train_set_size)

    def query_category_children(self, c_oid, level = 1):
        query =  'MATCH (c:CAT)<-[co:CHILD_OF*' + str(level) + ']-(c2:CAT) '
        query += 'WHERE c.oid=' + str(c_oid) + ' RETURN collect(c2.oid) AS children'
        return self.graph.data(query)

    def query_items_categories(self, data_type = 'train'):
        """
        Parameters
        --------------
        data_type
            'train' or 'test'

        Return
        --------------
        [{'cats': [int,..], 'p.oid': int, 'tf.timestamp': 'timestamp'},..]
        """
        return self._query_db(
            '(tf:TIME_FRAME)<-[:CREATED_AT]-(p:PRODUCT)-[df:DEFINED]->(c:CAT)',
            'p.oid, collect(c.oid) AS cats, tf.timestamp ORDER BY tf.timestamp',
            None,
            data_type
        )

    def query_orders(self, data_type = 'train', order_by = 'user'):
        order = 'u.oid, o.oid'
        if order_by == 'order':
            order = 'o.oid, u.oid'

        return self._query_db(
            '(tf:TIME_FRAME)<-[:CREATED_AT]-(o:ORDER)-[cr:CONTAINS]->(p:PRODUCT)-[df:DEFINED]->(c:CAT), (o)<-[pr:PURCHASED]-(u:USER)',
            'u.oid AS user, o.oid AS order, p.oid AS item, collect(c.oid) AS cats, tf.timestamp AS timestamp ORDER BY ' + order,
            None,
            data_type
        )

    def query_items_cats(self, data_type = 'train'):
        return self._query_db(
            '(tf:TIME_FRAME)<-[:CREATED_AT]-(p:PRODUCT)-[:DEFINED]->(c:CAT)',
            'p.oid AS item, collect(c.oid) AS cats',
            None,
            data_type
        )

    def query_items(self, data_type = 'train'):
        return self._query_db(
            '(p:PRODUCT)-[:CREATED_AT]->(tf:TIME_FRAME)',
            'collect(distinct p.oid) AS items',
            None,
            data_type
        )

    def query_items_total_num(self, data_type = 'train'):
        return self._query_db(
            '(p:PRODUCT)-[:CREATED_AT]->(tf:TIME_FRAME)',
            'count(p) AS items_total_num',
            None,
            data_type
        )

    def query_users(self, data_type = 'train'):
        return self._query_db(
            '(u:USER)-[:CREATED_AT]->(tf:TIME_FRAME)',
            'collect(u.oid) AS users',
            None,
            data_type
        )

    def query_users_items(self, data_type = 'train'):
        return self._query_db(
            '(p:PRODUCT)<-[:CONTAINS]-(o:ORDER)<-[:PURCHASED]-(u:USER), (tf:TIME_FRAME)<-[:CREATED_AT]-(o)',
            'u.oid AS user, collect(p.oid) AS items ORDER BY u.oid',
            None,
            data_type
        )

    def query_items_users(self, data_type = 'train'):
        return self._query_db(
            '(p:PRODUCT)<-[:CONTAINS]-(o:ORDER)-[:CREATED_AT]->(tf:TIME_FRAME), (o)<-[:PURCHASED]-(u:USER)',
            'p.oid AS product, collect(u.oid) AS users ORDER BY p.oid',
            None,
            data_type
        )

    def query_categories(self):
        """
        Return
        --------------
        {cat_oid: [parent_cat_oid,..],..}
        where ach dict key is an category oid holding a list of parent cats ids
        sorted on a level where parent[0] is closests parent
        """
        # get the categories from db
        query = 'MATCH (c:CAT) RETURN c.oid AS cat_oid ORDER BY c.oid'
        root_cats = self.graph.data(query)
        cats = {}
        for root_cat in root_cats:
            cats[int(root_cat['cat_oid'])] = []
        # get parents of each class for x levels
        is_parent_available = True
        level_c = 0
        while is_parent_available:
            level_c += 1
            query =  'MATCH path=(c:CAT)-[:CHILD_OF*' + str(level_c) + ']->(c2:CAT) '
            query += 'RETURN c.oid AS cat_oid, c2.oid AS parent_oid'
            parent_cats = self.graph.data(query)

            if len(parent_cats) is 0:
                is_parent_available = False
            else:
                for cat in parent_cats:
                    cats[cat['cat_oid']].append(cat['parent_oid'])

        return cats


    def query_order_time_distribution(self, data_type = 'train'):
        return self._query_db(
            '(tf:TIME_FRAME) OPTIONAL MATCH (tf)<-[:CREATED_AT]-(o:ORDER)',
            'tf.timestamp AS timestamp, o.oid AS order ORDER BY tf.timestamp',
            None,
            data_type
        )

    def query_user_time_distribution(self, data_type = 'train'):
        return self._query_db(
            '(tf:TIME_FRAME) OPTIONAL MATCH (tf)<-[:CREATED_AT]-(u:USER)',
            'tf.timestamp AS timestamp, u.oid AS user ORDER BY tf.timestamp',
            None,
            data_type
        )

    def query_first_purchase_dates(self, data_type = 'train'):
        return self._query_db(
            '(u:USER)-[:PURCHASED*0..1]->(o:ORDER)-[:CREATED_AT]->(tf:TIME_FRAME)',
            'u.oid AS user, collect(tf.timestamp)[0] AS first_purchase ORDER BY first_purchase',
            None,
            data_type
        )

    def query_timestamps(self, data_type = 'train'):
        return self._query_db(
            '(tf:TIME_FRAME)',
            'tf.timestamp AS timestamp ORDER BY timestamp',
            None,
            data_type
        )

    def query_items_per_order(self, data_type = 'train'):
        return self._query_db(
            '(tf:TIME_FRAME)<-[:CREATED_AT]-(o:ORDER)-[ct:CONTAINS]->(p:PRODUCT) WITH o, count(ct) AS items_num',
            'count(o.oid) AS orders_num, items_num ORDER BY items_num',
            None,
            data_type
        )

    def query_orders_total(self, data_type = 'train'):
        return self._query_db(
            '(o:ORDER)-[:CREATED_AT]->(tf:TIME_FRAME)',
            'count(o) AS orders_total',
            None,
            data_type
        )

    def query_items_total_for_part_of_day(self, part_of_day, data_type = 'train'):
        return self._query_db(
            '(p:PRODUCT)-[:CONTAINS]-(o:ORDER)-[:CREATED_AT]-(tf:TIME_FRAME) ',
            'count(p) AS items_total',
            'tf.part_of_day="' + part_of_day + '"',
            data_type
        )

    def query_items_total_for_day_in_week(self, day_in_week, data_type = 'train'):
        return self._query_db(
            '(p:PRODUCT)-[:CONTAINS]-(o:ORDER)-[:CREATED_AT]-(tf:TIME_FRAME) ',
            'count(p) AS items_total',
            'tf.day_in_week="' + day_in_week + '"',
            data_type
        )


    def set_graph(self, config_path):
        """
        Parameters
        --------------
        config_path
            path to a config.json with data needed to connect to a db

        Return
        --------------
        None
        """
        with open(config_path) as config_data:
            config = json.load(config_data)
            [str(x) for x in config]
            host = config['host']

            db_url = 'http://'
            if host['use_ssl']:
                db_url = 'https://'
            db_url += host['address'] + ':' + str(host['port']) + '/' + host['data_path']

            authenticate(host['address'] + ':' + str(host['port']), host['username'], host['password'])
            self.graph = Graph(db_url)

    def get_graph(self):
        """
        Return
        --------------
        Graph instance
        """
        if self.graph is None:
            if type(self.config_path) is str:
                self.set_graph(self.config_path)
            else:
                return None
        return self.graph

    def set_test_size(self, train_set_size = 0.7):
        self._set_last_train_tf(train_set_size)

    def _query_db(self, match_nodes, return_values, where_conditional = None, data_type = 'train'):
        query =  'MATCH ' + match_nodes + ' '

        if where_conditional is not None and len(where_conditional) > 0:
            query += 'WHERE ' + where_conditional + ' '

        if data_type is not 'all':
            op = self._get_timestamp_operator(data_type)
            if where_conditional is not None and len(where_conditional) > 0:
                query += 'AND '
            else:
                query += 'WHERE '
            query += 'tf.timestamp ' + op + ' "' + self.last_train_tf['timestamp'] + '" '

        query += 'RETURN ' + return_values
        return self.graph.data(query)

    def _set_last_train_tf(self, train_set_size):
        # get total numbers of non-empty (just in case!) orders
        orders_c = self.graph.data('MATCH (o:ORDER) WHERE (o)-[:CONTAINS]->() RETURN count(o) AS o_c')
        orders_c = orders_c[0]
        if 'o_c' in orders_c:
            orders_c = orders_c['o_c']
        else:
            return
        # get time frame connected to last train order - edge TF
        last_train_order_index = int(math.ceil(train_set_size * orders_c))
        rel_c = 0
        tf_rels = self.graph.data('MATCH (o:ORDER)-[:CREATED_AT]->(tf:TIME_FRAME) WHERE (o)-[:CONTAINS]->() RETURN tf')
        for rel in tf_rels:
            rel_c += 1
            if rel_c == last_train_order_index:
                self.last_train_tf = rel['tf']
                return self.last_train_tf
        return

    def _get_timestamp_operator(self, data_type):
        """
        Parameters
        --------------
        data_type
            'test' or 'train'

        Return
        --------------
        String
        """
        if data_type == 'train':
            return '<='
        elif data_type == 'test':
            return '>'
        else:
            return ''

    def _get_tfs(self, data_type):
        if self.last_train_tf is None:
            self._set_last_train_tf(0.7)

        query =  'MATCH (tf:TIME_FRAME) '
        query += 'WHERE tf.timestamp ' + self._get_timestamp_operator(data_type)
        query += ' "' + self.last_train_tf['timestamp'] + '" '
        query += 'RETURN tf ORDER BY tf.timestamp'
        return self.graph.data(query)
