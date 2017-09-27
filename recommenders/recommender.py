import numpy as np

class RecommenderBase(object):

    def __init__(self, attrs = None, items = None, users = None, items_attrs = None, users_items = None):
        self.set_train_data(attrs, items, users, items_attrs, users_items)

    def __init__(self):
        self.log = None

    def get_model(self):
        """
        Return
        --------------
        {user_id: {user_attrs: [cat_id,...], known: [item_id,...]},...}
        """
        try:
            return self.model
        except AttributeError:
            return None

    def get_recommendations(self):
        """
        Return
        --------------
        {user_id: [item_id, item_id,...],...}
        """
        try:
            return self.recommendations
        except AttributeError:
            return None

    def set_k(self, k):
        try:
            self.k = int(k)
        except (ValueError, TypeError):
            self.k = None
        return self.k

    def get_k(self):
        try:
            return self.k
        except AttributeError:
            return None

    def _get_fallback_items(self, fallback_model):
        if fallback_model is not None:
            fallback_recomm = fallback_model(self.train_data['items'], self.train_data['users'], self.train_data['items_users'], self.train_data['users_items'])
            return fallback_recomm.train()

        return None

    def set_logger(self, logger = None):
        if logger is not None:
            self.log = logger

    def _append_fallback_items(self, fallback_items, recommendations):
        free_recomm_slots = self.get_k() - len(recommendations)
        for fallback_item in fallback_items:
            if fallback_item not in recommendations:
                recommendations.append(fallback_item)
                free_recomm_slots -= 1
            if free_recomm_slots <= 0:
                return recommendations

        return recommendations

    def _transform_user_input_data(self, user_data):
        if type(user_data) is list:
            return np.array(user_data)
        elif type(user_data) is np.ndarray:
            return user_data
        return None
