import numpy as np
from recommender import RecommenderBase

class PopularItemsRecommender(RecommenderBase):

    def __init__(self, items = None, users = None, items_users = None, users_items = None):
        super(PopularItemsRecommender, self).__init__()
        self.set_train_data(items, users, items_users, users_items)

    def train(self):
        self.model = []

        items_users_count = np.zeros(len(self.train_data['items_users']))
        item_i = 0
        for item_users in self.train_data['items_users']:
            items_users_count[item_i] = len(item_users.nonzero()[0])

            item_i += 1

        for item_i in items_users_count.argsort()[::-1]:
            self.model.append(self.train_data['items'][item_i])

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

        user_i = 0
        for user_items in self.train_data['users_items']:
            known_items = []
            for user_item_i in user_items.nonzero()[0]:
                known_items.append(self.train_data['items'][user_item_i])

            self.recommendations[ self.train_data['users'][user_i] ] = {
                'recommended': self.model[0:self.get_k()],
                'known_items': known_items
            }
            user_i += 1

        return self.recommendations

    def set_train_data(self, items, users, items_users, users_items):
        self.train_data = {}

        self.train_data['items']       = self._transform_user_input_data(items)
        self.train_data['users']       = self._transform_user_input_data(users)
        self.train_data['items_users'] = self._transform_user_input_data(items_users)
        self.train_data['users_items'] = self._transform_user_input_data(users_items)
