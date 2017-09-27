from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics.pairwise import pairwise_distances
from scipy.spatial import distance
from helpers.decorators import deprecated
from recommender import RecommenderBase

import numpy as np

class CbfRecommender(RecommenderBase):

    def __init__(self, attrs = None, items = None, users = None, items_attrs = None, users_items = None):
        super(CbfRecommender, self).__init__()
        self.set_train_data(attrs, items, users, items_attrs, users_items)

    def train(self):
        """
        Creates a model which represents users personal affinities.

        Parameters
        --------------
        none

        Return
        --------------
        {user_id: {'user_attrs':[float,...]},...}
        """
        self.model = {}

        transformer = TfidfTransformer()
        # item index = 0 cat index = 1
        items_attrs_weighted = transformer.fit_transform(self.train_data['item_attrs'])
        for user_i in range(0, self.train_data['users_items'].shape[0]):
            user_attrs = None
            known_items = []
            for product_i in np.nonzero(self.train_data['users_items'][user_i]):
                known_items.append( self.train_data['items'][product_i] )
                if user_attrs is None:
                    user_attrs = items_attrs_weighted[product_i]
                else:
                    user_attrs += items_attrs_weighted[product_i]

            self.model[ self.train_data['users'][user_i] ] = {
                'known_items': known_items,
                'user_attrs': user_attrs
            }

        return self.model

    def generate_recommendations(self, k = None, distance_metric = 'manhattan'):
        try:
            if type(self.model) != dict:
                return False
        except AttributeError:
            self.train()

        if k is not None:
            self.set_k(k)

        users_attrs = []
        for user_id in self.model.keys():
            users_attrs.append(self.model[user_id]['user_attrs'].toarray()[0])
        users_attrs = np.array(users_attrs)

        transformer = TfidfTransformer()
        items_attrs_weighted = transformer.fit_transform(self.train_data['item_attrs'])
        # 0 - user_i, 1 - item_i
        # http://scikit-learn.org/stable/modules/generated/sklearn.metrics.pairwise.pairwise_distances.html
        users_items_affinity = pairwise_distances(users_attrs, items_attrs_weighted.toarray(), distance_metric)

        user_i = 0
        self.recommendations = {}
        for user_id in self.model.keys():
            self.recommendations[user_id] = {
                'known_items': self.model[user_id]['known_items'][0],
                'recommended': []
            }
            for item_i in users_items_affinity[user_i].argsort()[0:self.get_k()]:
                self.recommendations[user_id]['recommended'].append(self.train_data['items'][item_i])
            user_i += 1
        return self.recommendations

    @deprecated('outdated method')
    def train_by_attrs(self):
        try:
            transformer = TfidfTransformer()
            # shape (num_of_products, num_of_cats)
            item_cats_weighted = transformer.fit_transform(self.train_data['item_attrs'])
            self.model = {}
            for user_index in range(0, len(self.train_data['users'])):
                item_indices = np.nonzero(self.train_data['users_items'][user_index])[0]
                user_attrs_affinity = None
                known_items = []
                for item_index in item_indices:
                    known_items.append(self.train_data['items'][item_index])
                    if user_attrs_affinity is None:
                        user_attrs_affinity = item_cats_weighted[item_index]
                    else:
                        user_attrs_affinity += item_cats_weighted[item_index]

                if user_attrs_affinity is not None:
                    items_weighted = []
                    item_c = 0
                    for item_cat in item_cats_weighted:
                        items_sim = distance.euclidean(user_attrs_affinity.toarray(), item_cat.toarray())
                        items_weighted.append({'id': self.train_data['items'][item_c], 'sim': items_sim})

                        item_c += 1

                    items_weighted.sort(key=lambda el:el['sim'])
                    recommendations = []
                    for item in items_weighted[0:self.get_k()]:
                        recommendations.append(item['id'])

                    self.model[ self.train_data['users'][user_index] ] = {
                        'known': known_items,
                        'recommendations': recommendations
                    }
                else:
                    self.model[ self.train_data['users'][user_index] ] = None

            return True
        except AttributeError:
            return False

    @deprecated('outdated method')
    def train_by_products(self):
        try:
            transformer = TfidfTransformer()
            # shape (num_of_products, num_of_cats)
            tfidf_matrix = transformer.fit_transform(self.train_data['item_attrs'])
            # type scipy.sparse.csr.csr_matrix, shape (num_of_products, num_of_products)
            items_sim = cosine_similarity(tfidf_matrix, tfidf_matrix, False)

            self.model = {}
            for user_index in range(0, len(self.train_data['users'])):
                item_indices = np.nonzero(self.train_data['users_items'][user_index])[0]
                user_items_affinity = None
                known_items = []
                # get users affinity to each item by the relevance of items to the already bought items
                for item_index in item_indices:
                    known_items.append(self.train_data['items'][item_index])
                    user_item_affinity = np.multiply(items_sim.getcol(item_index), self.train_data['users_items'][user_index][item_index])
                    if user_items_affinity is None:
                        user_items_affinity = user_item_affinity
                    else:
                        user_items_affinity += user_item_affinity
                    # exclude from affinity matrix already known items
                    # user_items_affinity[item_index,0] = -1

                if user_items_affinity is None:
                    self.model[ self.train_data['users'][user_index] ] = None
                else:
                    sorted_indices = user_items_affinity.toarray().argsort(None)
                    recommendations = np.flipud(sorted_indices[-self.get_k():]).tolist()
                    self.model[ self.train_data['users'][user_index] ] = {
                        'known': known_items,
                        'recommendations': recommendations
                    }

            return True
        except AttributeError:
            return False

        return False

    def set_train_data(self, attrs, items, users, items_attrs, users_items):
        self.train_data = {}

        self.train_data['attrs']      = self._transform_user_input_data(attrs)
        self.train_data['items']      = self._transform_user_input_data(items)
        self.train_data['item_attrs'] = self._transform_user_input_data(items_attrs)

        self.train_data['users']       = self._transform_user_input_data(users)
        self.train_data['users_items'] = self._transform_user_input_data(users_items)
