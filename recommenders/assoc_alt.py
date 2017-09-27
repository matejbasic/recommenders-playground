from itertools import combinations
from itertools import izip_longest
from operator import itemgetter
from recommender import RecommenderBase
from pi import PopularItemsRecommender

import numpy as np

class AssociationRecommender(RecommenderBase):

    def __init__(self, orders = None, items = None, orders_items = None):
        super(AssociationRecommender, self).__init__()
        self.set_train_data(orders, items, orders_items)

    def train(self, min_support = 0.3, min_confidence = 0.7, equalize_rule_dim = True, equalize_missing_value = 0 ):
        """
        Creates a model which contains association rules for given items.

        Parameters
        --------------
        min_support
        min_confidence
        equalize_rule_dim
            shoud all the rule arrays have a same dimension
        equalize_missing_value
            which value should be put in a place of a non-existing value

        Return
        --------------
        [{'x': [int,..], 'y': [int,..], 'confidence': int},..]
        """
        self.model = []

        candidates = self.generate_candidates(min_support)
        self.model = self.generate_rules(candidates, min_confidence, equalize_rule_dim, equalize_missing_value)

        return self.model

    def generate_recommendations(self, k = None):
        try:
            if type(self.model) != list:
                return False
        except AttributeError:
            self.train()

        if k is not None:
            self.set_k(k)

        self.recommendations = {}

        # use items from fallback model if there's not enough of similar items
        pir = PopularItemsRecommender(self.train_data['items'], self.train_data['users'], self.train_data['items_users'], self.train_data['users_items'])
        fallback_items = pir.train()

    def generate_rules(self, candidates, min_confidence = 0.7, equalize_rule_dim = True, equalize_missing_value = 0):
        rules = {'x': [], 'x_len': [], 'y': [], 'y_len': [], 'confidence': []}
        for candidate_itemsets in candidates:
            for candidate_itemset in candidate_itemsets:
                possible_rules = self._get_possible_rules(candidate_itemset)
                for possible_rule in possible_rules:
                    rule_confidence = self._get_confidence(possible_rule['x'], possible_rule['y'])
                    if rule_confidence > min_confidence:
                        rules['confidence'].append(rule_confidence)
                        # replace item indices with item ids!
                        x_item_ids = self._get_item_id_by_index(possible_rule['x'])
                        y_item_ids = self._get_item_id_by_index(possible_rule['y'])
                        rules['x'].append( x_item_ids )
                        rules['y'].append( y_item_ids )
                        rules['x_len'].append( len(x_item_ids) )
                        rules['y_len'].append( len(y_item_ids) )

        if equalize_rule_dim:
            rules['x'] = self._equalize_list(rules['x'], equalize_missing_value)
            rules['y'] = self._equalize_list(rules['y'], equalize_missing_value)

        confidence_argsort = np.array(rules['confidence']).argsort()[::-1]
        rules['confidence'] = np.array(rules['confidence'])[confidence_argsort]

        rules['x'] = np.array(rules['x'])[confidence_argsort]
        rules['y'] = np.array(rules['y'])[confidence_argsort]

        return rules

    def generate_candidates(self, min_support):
        # step 1. generate single frequent items
        orders_num = self._get_orders_total()
        single_frequent_items = self._get_single_frequent_items(min_support, orders_num)

        # step 2. generate paired frequent itemsets
        candidates = [[]]
        for item_i in range(single_frequent_items.shape[0]):
            for item2_i in range(item_i+1, single_frequent_items.shape[0]):
                candidate_support = self._get_support([single_frequent_items[item_i], single_frequent_items[item2_i]], orders_num)
                if candidate_support >= min_support:
                    candidates[0].append( [single_frequent_items[item_i], single_frequent_items[item2_i]] )

        # step 3. generate >2 frequent itemsets
        i = 1
        while len(candidates[i-1]) > 1:
            candidates.append([])
            for candidate_i in range(len(candidates[i-1])):
                for candidate2_i in range(candidate_i, len(candidates[i-1])):
                    if candidates[i-1][candidate_i] != candidates[i-1][candidate2_i]:
                        new_candidate = self._try_to_merge(candidates[i-1][candidate_i], candidates[i-1][candidate2_i])
                        if new_candidate != False:
                            new_candidate_support = self._get_support(new_candidate, orders_num)
                            if new_candidate_support > min_support and new_candidate not in candidates[i]:
                                candidates[i].append(new_candidate)
            i += 1

        return candidates

    def set_train_data(self, orders, items, orders_items):
        self.train_data = {}

        self.train_data['orders']       = self.transform_user_input_data(orders)
        self.train_data['items']        = self.transform_user_input_data(items)
        self.train_data['orders_items'] = self.transform_user_input_data(orders_items)

    def _equalize_list(self, the_list, missing_value):
        """
        Creates a list of same dimensional sublists

        Parameters
        --------------
        the_list
            should be a list of list, [[],..]
        missing_value
            if a sublist is shorter than a longest sublist, the missing_value will
            be appended to the sublist
        Return
        --------------
        [[]]
        """
        list_dim = max(len(l) for l in the_list)
        if list_dim < 2:
            return the_list
        for sublist_i in range(0, len(the_list)):
            the_list[sublist_i] += [missing_value] * (list_dim - len(the_list[sublist_i]))

        return the_list

    def _get_item_id_by_index(self, indices):
        ids = []
        if type(indices) is int:
            indices = [indices]
        for index in indices:
            ids.append(self.train_data['items'][index])

        return ids

    def _get_orders_total(self):
        return self.train_data['orders'].shape[0]

    def _try_to_merge(self, itemset1, itemset2):
        same_part = []
        diff_part = []
        different_item_found = False
        for item1 in itemset1:
            if item1 in itemset2:
                same_part.append(item1)
            elif different_item_found == False:
                different_item_found = True
                diff_part.append(item1)
            else:
                # >1 different item, candidate is off!
                return False

        for item2 in itemset2:
            if item2 not in same_part:
                diff_part.append(item2)
                break
        merged_itemset = same_part + diff_part
        merged_itemset.sort()

        return merged_itemset

    def _get_possible_rules(self, candidate):
        """
        Creates a list of possible rules from given candidate itemset

        Parameters
        --------------
        candidate

        Return
        --------------
        [{'x':[int,..], 'y':[int,..]},..]
        """
        ys = sum([map(list, combinations(candidate, i)) for i in range(1, len(candidate))], [])
        rules = []
        for y in ys:
            rules.append({'x': [x for x in candidate if x not in y], 'y': y})

        return rules

    def _get_confidence(self, rule_x, rule_y):
        b_support_c = self._get_support_count(rule_x)
        if b_support_c == 0:
            return 0

        a_support_c = self._get_support_count(rule_x + rule_y)
        if a_support_c == 0:
            return 0

        return a_support_c / float(b_support_c)

    def _get_support_count(self, itemset):
        support_c = 0
        for order_items in self.train_data['orders_items']:
            exists_in_order = True # if all of the items are present in order
            for item_i in itemset:
                if order_items[item_i] <= 0.0:
                    exists_in_order = False;
                    break
            if exists_in_order:
                support_c += 1
        return support_c

    def _get_support(self, candidate, orders_num):
        support_c = self._get_support_count(candidate)
        if support_c > 0:
            return support_c / float(orders_num)
        else:
            return 0

    def _get_single_frequent_items(self, min_support, orders_num):
        """
        Creates a array of frequent item indices
        """
        items_support_count = np.sum(self.train_data['orders_items'], axis=0)
        items_support       = np.divide(items_support_count, orders_num)
        frequent_items      = np.where( items_support >= min_support )

        if type(frequent_items) is tuple:
            return frequent_items[0]
        else:
            return frequent_items
