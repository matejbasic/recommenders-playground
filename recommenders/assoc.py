from fim import apriori, fpgrowth, eclat
from recommender import RecommenderBase
from pi import PopularItemsRecommender
from operator import itemgetter
import numpy as np

class AssociationRecommender(RecommenderBase):

    supported_algorithms = [
        ('apriori', apriori),
        ('fpgrowth', fpgrowth),
        ('eclat', eclat)
    ]

    def __init__(self, orders = None, items = None, users = None, items_users = None, users_items = None):
        if orders is not None:
            self.set_train_data(orders, items, users, items_users, users_items)

    def train(self, min_support = 0.3, min_confidence = 0.8, algorithm = 'apriori', equalize_rule_dim = True, equalize_missing_value = 0):
        min_support    *= 100
        min_confidence *= 100

        alg_function = self._get_algorithm_function(algorithm)

        gen_rules = alg_function(self.train_data['orders'], target='r', supp=min_support, conf=min_confidence, report='r', eval='s')
        gen_rules.sort(key=itemgetter(2), reverse=True)

        rules = {'x': [], 'x_len': [], 'y': [], 'y_len': [], 'confidence': []}
        rule_dim_max = 0
        for rule in gen_rules:
            rule_dim = len(rule[1])
            if rule_dim:
                if rule_dim > rule_dim_max:
                    rule_dim_max = rule_dim
                if type(rule[0]) is int:
                    rules['x'].append([rule[0]])
                    rules['x_len'].append(1)
                elif type(rule[0]) is tuple:
                    rules['x'].append(list(rule[0]))
                    rules['x_len'].append(len(rule[0]))
                rules['y'].append(list(rule[1]))
                rules['y_len'].append(rule_dim)
                rules['confidence'].append(rule[2])

        if equalize_rule_dim:
            rules['x'] = self._equalize_list(rules['x'], equalize_missing_value)
            rules['y'] = self._equalize_list(rules['y'], equalize_missing_value)

        for key in ('x', 'y', 'confidence'):
            rules[key] = np.array(rules[key])

        self.model = rules
        return self.model

    def generate_recommendations(self, k = None, fallback_model = PopularItemsRecommender):
        try:
            if type(self.model) != dict:
                return False
        except AttributeError:
            self.train()

        if k is not None:
            self.set_k(k)

        self.recommendations = {}

        if fallback_model is not None:
            fallback_items = self._get_fallback_items(fallback_model)
        else:
            fallback_items = None

        user_i = 0
        for user_items in self.train_data['users_items']:
            recommendations = []
            recommendation_c = 0
            known_items = []

            for user_item_i in user_items.nonzero()[0]:
                item_id = self.train_data['items'][user_item_i]
                known_items.append(item_id)
                model_raw_indices = np.where(self.model['x']==item_id)
                model_indices = []
                for rules_i in model_raw_indices:
                    model_indices += rules_i.tolist()

                model_indices.sort()
                model_indices = set(model_indices)
                for key in ['y', 'x']:
                    for rule_item_i in model_indices:
                        for item_id in self.model[key][rule_item_i]:
                            if item_id != 0 and recommendation_c <= self.get_k() and item_id not in recommendations:
                                recommendations.append(item_id)
                                recommendation_c += 1

            if recommendation_c < self.get_k() and fallback_items is not None:
                recommendations = self._append_fallback_items(fallback_items, recommendations)

            self.recommendations[ self.train_data['users'][user_i] ] = {
                'recommended': recommendations,
                'known_items': known_items
            }
            user_i += 1

        return self.recommendations

    def _get_algorithm_function(self, algorithm_name):
        for alg in self.supported_algorithms:
            if alg[0] == algorithm_name:
                return alg[1]

        return apriori

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

        if len(the_list) == 0:
            return the_list

        list_dim = max(len(l) for l in the_list)
        if list_dim < 2:
            return the_list
        for sublist_i in range(0, len(the_list)):
            the_list[sublist_i] += [missing_value] * (list_dim - len(the_list[sublist_i]))

        return the_list

    def set_train_data(self, orders, items, users, items_users, users_items):
        self.train_data = {}
        self.train_data['orders'] = []

        current_order_id    = -1
        current_order_items = []
        for order in orders:
            if order['order'] != current_order_id:
                self.train_data['orders'].append(current_order_items)
                current_order_id = order['order']
                current_order_items = []

            current_order_items.append(order['item'])

        self.train_data['items']       = self._transform_user_input_data(items)
        self.train_data['users']       = self._transform_user_input_data(users)
        self.train_data['items_users'] = self._transform_user_input_data(items_users)
        self.train_data['users_items'] = self._transform_user_input_data(users_items)
