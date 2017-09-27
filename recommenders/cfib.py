import math
import numpy as np
from recommender import RecommenderBase
from pi import PopularItemsRecommender
from sklearn.metrics.pairwise import pairwise_distances
from scipy.spatial.distance import euclidean

class CfItemBasedRecommender(RecommenderBase):

    def __init__(self, items = None, users = None, items_users = None, users_items = None):
        super(CfItemBasedRecommender, self).__init__()
        self.set_train_data(items, users, items_users, users_items)

    def train(self):
        """
        Creates a model which represents similar items by the users who bought them.

        Parameters
        --------------
        none

        Return
        --------------
        [item_id:{'similar_items':[item_id,...]},...]
        """
        self.model = {}

        item_i = 0
        items_similarity = []
        # calculate item similarity
        for item_users in self.train_data['items_users']:
            item_users      = item_users.nonzero()[0]
            item_similiarty = []
            if len(item_users) == 0:
                item_similiarty = [0.0] * len(self.train_data['items_users'])
                item_similiarty[item_i] = 1.0
            else:
                matched_item_weight = 1.0 / len(item_users)
                for item_users_2 in self.train_data['items_users']:
                    # basically same as jaccard distance
                    sim = np.sum(np.in1d(item_users, item_users_2.nonzero()) * matched_item_weight)
                    item_similiarty.append(sim)

            items_similarity.append(np.array(item_similiarty))
            item_i += 1

        item_i = 0
        # extract similar items
        for item_similiarty in items_similarity:
            similar_items = []
            for similar_item_i in item_similiarty.argsort()[::-1]:
                if item_similiarty[similar_item_i] > 0.0:
                    similar_items.append(self.train_data['items'][similar_item_i])
                else:
                    break

            self.model[self.train_data['items'][item_i]] = {
                'similar_items': similar_items
            }
            item_i += 1

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
        for user_items in self.train_data['users_items']:
            recommendations_per_item = int(math.ceil(float(self.get_k()) / user_items.nonzero()[0].shape[0]))
            recommendations = []
            known_items = []

            recomm_item_position_step = 1
            for user_item_i in user_items.nonzero()[0]:
                known_items.append(self.train_data['items'][user_item_i])
                if len(recommendations) == 0:
                    recommendations = self.model[ self.train_data['items'][user_item_i] ]['similar_items'][0:recommendations_per_item]
                else:
                    recommendation_c = 0
                    for recomm_item_id in self.model[ self.train_data['items'][user_item_i] ]['similar_items']:
                        if recomm_item_id not in recommendations:
                            recommendation_c += 1
                            recommendations.insert(recommendation_c * recomm_item_position_step, recomm_item_id)
                        if recommendation_c >= recommendations_per_item:
                            break
                recomm_item_position_step += 1

            if fallback_items is not None and len(recommendations) < self.get_k():
                recommendations = self._append_fallback_items(fallback_items, recommendations)

            self.recommendations[ self.train_data['users'][user_i] ] = {
                'recommended': recommendations,
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
