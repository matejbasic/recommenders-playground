import math
import numpy as np
from recommender import RecommenderBase
from pi import PopularItemsRecommender
from sklearn.metrics import jaccard_similarity_score

class CfUserBasedRecommender(RecommenderBase):

    def __init__(self, items = None, users = None, items_users = None, users_items = None):
        super(CfUserBasedRecommender, self).__init__()
        self.set_train_data(items, users, items_users, users_items)

    def train(self):
        self.model = {}

        users_similiarty = []
        user_i = 0
        for user_items in self.train_data['users_items']:
            user_items      = user_items.nonzero()[0]
            user_similiarty = []
            # fetch known items
            self.model[self.train_data['users'][user_i]] = {'known_items': []}
            for item_i in user_items:
                self.model[ self.train_data['users'][user_i] ]['known_items'].append(self.train_data['items'][item_i])

            # find similarity between user's items and other users items
            if len(user_items) > 0:
                user_items_comp = [1.0] * len(user_items)
                for user_items_2 in self.train_data['users_items']:
                    user_items_2 = user_items_2.nonzero()
                    if len(user_items_2) > 0:
                        user_items_2_comp = np.in1d(user_items, user_items_2) * 1.0
                        user_similiarty.append(jaccard_similarity_score(user_items_comp, user_items_2_comp))
                    else:
                        user_similiarty.append(0.0)
            else:
                user_similiarty = [0.0] * len(self.train_data['users_items'])
                user_similiarty[user_i] = 1.0

            users_similiarty.append(np.array(user_similiarty))
            user_i += 1

        user_i = 0
        # extract similar users
        for user_similiarty in users_similiarty:
            similar_users = []
            for similar_user_i in user_similiarty.argsort()[::-1]:
                if user_similiarty[similar_user_i] > 0.0:
                    similar_users.append(self.train_data['users'][similar_user_i])
                else:
                    break

            self.model[self.train_data['users'][user_i]]['similar_users'] = similar_users
            user_i += 1

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

        # use items from fallback model if there's not enough of similar items
        if fallback_model is not None:
            fallback_items = self._get_fallback_items(fallback_model)
        else:
            fallback_items = None

        user_i = 0
        users_list = self.train_data['users'].tolist()
        for user_id in users_list:
            recommendations           = []
            recommendations_per_user  = int(math.ceil(float(self.get_k()) / len(self.model[user_id]['similar_users'])))
            recomm_item_position_step = 0

            # extract users items
            for similar_user_id in self.model[user_id]['similar_users']:
                recommendation_c = 0
                similar_user_items = self.train_data['users_items'][ users_list.index(similar_user_id) ].nonzero()[0]

                for item_i in similar_user_items:
                    if self.train_data['items'][item_i] not in recommendations:
                        recommendation_c += 1
                        recommendations.insert(recommendation_c * recomm_item_position_step, self.train_data['items'][item_i])

                    if recommendation_c >= recommendations_per_user:
                        break
                recomm_item_position_step += 1

            # fallback if there's not enough recommendations
            if fallback_items is not None and len(recommendations) < self.get_k():
                recommendations = self._append_fallback_items(fallback_items, recommendations)

            self.recommendations[user_id] = {
                'recommended': recommendations,
                'known_items': self.model[user_id]['known_items']
            }
            user_i += 1

        return self.recommendations

    def set_train_data(self, items, users, items_users, users_items):
        self.train_data = {}

        self.train_data['items']       = self._transform_user_input_data(items)
        self.train_data['users']       = self._transform_user_input_data(users)
        self.train_data['items_users'] = self._transform_user_input_data(items_users)
        self.train_data['users_items'] = self._transform_user_input_data(users_items)
