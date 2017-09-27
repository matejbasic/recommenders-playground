import math
from query_manager import QueryManager

class DataManager:

    def __init__(self, config_path, train_set_size = 0.7):
        if config_path is not None:
            self.qm = QueryManager(config_path, train_set_size)
        self.categories = None

        self.log = None

    def get_user_items(self, data_type = 'train', item_aggr_sum = False):
        """
        Parameters
        --------------
        data_type
            'train', 'test' or 'all'
        item_aggr_sum
            if True, 'users_items' contains total number of connections from user to an item,
            otherwise 'users_items' is binary matrix

        Return
        --------------
        {'items': [int,..], 'users': [int,..], 'users_items':[[int,..],..]}
        """
        if self._is_data_available('_ui_data', data_type):
            return self._ui_data[data_type]

        items           = self.qm.query_items(data_type)[0]['items']
        users_items_raw = self.qm.query_users_items(data_type)

        template_items = [0.0] * len(items)
        self._ui_data[data_type] = {'items': items, 'users': [], 'users_items': []}

        user_i = -1
        for user_items in users_items_raw:
            self._ui_data[data_type]['users'].append(user_items['user'])
            user_i += 1
            self._ui_data[data_type]['users_items'].append(list(template_items))
            for user_item in user_items['items']:
                if item_aggr_sum:
                    self._ui_data[data_type]['users_items'][user_i][items.index(user_item)] += 1
                else:
                    self._ui_data[data_type]['users_items'][user_i][items.index(user_item)] = 1

        return self._ui_data[data_type]

    def get_orders(self, data_type = 'train'):
        """
        Parameters
        --------------
        data_type
            'train', 'test' or 'all'

        Return
        --------------
        {'items': [int,..], 'orders': [int,..], 'orders_items':[[int,..],..]}
        """
        if self._is_data_available('_o_data', data_type):
            return self._o_data[data_type]

        self._o_data[data_type] = self.qm.query_orders(data_type, 'order')
        return self._o_data[data_type]

    def get_order_items(self, data_type = 'train'):
        """
        Parameters
        --------------
        data_type
            'train', 'test' or 'all'

        Return
        --------------
        {'items': [int,..], 'orders': [int,..], 'orders_items':[[int,..],..]}
        """
        if self._is_data_available('_oi_data', data_type):
            return self._oi_data[data_type]

        orders_raw = self.qm.query_orders(data_type)
        items      = self.qm.query_items('all')[0]['items']
        template_items = [0.0] * len(items)
        self._oi_data[data_type] = {'items': items, 'orders': [], 'orders_items': []}

        current_order_id    = -1
        current_order_i     = -1
        for order_item in orders_raw:
            try:
                if current_order_id != order_item['order']:
                    current_order_id    = order_item['order']
                    current_order_i += 1
                    self._oi_data[data_type]['orders'].append(current_order_id)
                    self._oi_data[data_type]['orders_items'].append(list(template_items))

                self._oi_data[data_type]['orders_items'][current_order_i][items.index(order_item['item'])] += 1
            except ValueError:
                print 'ORDER: Item not found: ' + str(order_item['item'])

        return self._oi_data[data_type]

    def get_items_users(self, data_type = 'train'):
        """
        Parameters
        --------------
        data_type
            'train', 'test' or 'all'

        Return
        --------------
        {item_id: {user_id: count,...},...}
        """
        if self._is_data_available('_iu_data', data_type):
            return self._iu_data[data_type]

        items_users = self.qm.query_items_users(data_type)
        items       = self.qm.query_items_categories('all')
        users_items = self.qm.query_users_items(data_type)
        users       = []

        for user_items in users_items:
            users.append(user_items['user'])

        template_users = [0.0] * len(users)
        self._iu_data[data_type] = {'items': [], 'users': users, 'items_users': []}

        for item in items:
            has_item_users = False
            item_users_matrix = list(template_users)
            self._iu_data[data_type]['items'].append(item['p.oid'])
            for item_users in items_users:
                if item['p.oid'] == item_users['product']:
                    has_item_users = True
                    for user in item_users['users']:
                        item_users_matrix[users.index(user)] += 1
                        item_users_matrix[users.index(user)] = 1
                    self._iu_data[data_type]['items_users'].append(item_users_matrix)
                    break

            if has_item_users is False:
                self._iu_data[data_type]['items_users'].append(item_users_matrix)

        return self._iu_data[data_type]

    def get_users_items_cats(self, data_type = 'train'):
        """
        Parameters
        --------------
        data_type
            'train', 'test' or 'all'

        Return
        --------------
        { 'items': [int,..], 'item_cats': [[int,...],..], 'users': [int,..],
          'user_items': [[int,...],..], 'cats': [int,...] }
        where 'items' are ids of the items and 'item_cats' are weights for each
        category and item accordingly to 'items' id, they share same list index
        """
        if self._is_data_available('_uic_data', data_type):
            return self._uic_data[data_type]

        # query data from db
        self.cats   = self.qm.query_categories() # no connected TFs
        items       = self.qm.query_items_categories('all')
        users_items = self.qm.query_users_items(data_type)
        # return dict
        self._uic_data[data_type] = {'items': [], 'item_cats': [], 'users': [], 'user_items': [], 'cats': []}
        # templates
        template_item_cats  = [0.0] * len(self.cats)
        template_user_items = [0.0] * len(items)
        item_w              = 1.0
        item_w_parent_cost  = item_w / 4 # TODO explain this magic num?

        # categories
        for cat_oid in self.cats:
            self._uic_data[data_type]['cats'].append(cat_oid)

        for item in items:
            # items without categories are useless
            if len(item['cats']) < 1:
                continue
            # items
            self._uic_data[data_type]['items'].append(int(item['p.oid']))

            # define item categories
            item_cats_matrix = list(template_item_cats)
            for cat_defined_oid in item['cats']:
                cat_w = item_w
                item_cats_matrix[self._uic_data[data_type]['cats'].index(cat_defined_oid)] = round(cat_w, 2)
                # parent level is defined by list's index
                # e.g. cat's parent has index 0, it's parent 1,...
                # TODO test diff strategies for pondering
                for cat_parent in self.cats[cat_defined_oid]:
                    # if written w is smaller write the new w, favorite close connections
                    if item_cats_matrix[self._uic_data[data_type]['cats'].index(cat_defined_oid)] < cat_w:
                        item_cats_matrix[self._uic_data[data_type]['cats'].index(cat_defined_oid)] = round(cat_w, 4)
                    cat_w -= item_w_parent_cost
            self._uic_data[data_type]['item_cats'].append(item_cats_matrix)

        # create user items matrices
        for user_items in users_items:
            self._uic_data[data_type]['users'].append(int(user_items['user']))

            user_item_matrix = list(template_user_items)
            for user_item in user_items['items']:
                try:
                    item_index = self._uic_data[data_type]['items'].index(int(user_item))
                except ValueError:
                    continue
                user_item_matrix[item_index] += round(item_w, 2)

            self._uic_data[data_type]['user_items'].append(user_item_matrix)
        return self._uic_data[data_type]

    def get_items(self, data_type = 'train'):
        """
        Parameters
        --------------
        data_type
            'train', 'test' or 'all'

        Return
        --------------
        {[product: int, cats: [int,...],...],}
        """
        if self._is_data_available('_i_data', data_type):
            return self._i_data[data_type]

        self._i_data[data_type] = self.qm.query_items_cats(data_type)
        return self._i_data[data_type]

    def get_items_total_num(self, data_type = 'train'):
        total_num = self.qm.query_items_total_num(data_type)
        return total_num[0]['items_total_num']

    def set_logger(self, logger = None):
        if logger is not None:
            self.log = logger

    def _is_data_available(self, data_name, data_type, expected_py_type = dict):
        try:
            if type(getattr(self, data_name)) is not expected_py_type:
                setattr(self, data_name, expected_py_type())
                return False
            elif getattr(self, data_name)[data_type]:
                return True
        except (AttributeError, TypeError, KeyError):
            setattr(self, data_name, expected_py_type())
        except NameError:
            pass

        return False
