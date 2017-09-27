import time
import numpy as np
from base_tester import BaseTester

class AssociationTester(BaseTester):

    def __init__(self, recommendations = None, k = None):
        if recommendations is not None:
            self.set_recommendations(recommendations)
        if k is not None:
            self.set_k(k)

        self.log = None
        self.train_data = None
        self.transactions = None

    def test(self, model, k = None, model_type = ''):
        if self.transactions is None:
            return False

        if k is not None:
            self.set_k(k)

        confusion_matrix = {'tp': 0, 'tn': 0, 'fp': 0, 'fn': 0}

        current_t_id = -1
        current_t_items = []
        current_user = ''
        case_without_history_c = 0
        t_items_c = 0

        model_items_total = self.get_items_total(model)

        start = time.time()
        # should be ordered by order id
        for transaction in self.transactions:
            if transaction['order'] != current_t_id:
                # do transaction-wise operations HERE
                itemset = []
                for current_t_item in current_t_items:
                    t_items_c += 1
                    recommendations = []
                    # if itemset is 0 there's no items to make a prediction on for a current one
                    if len(itemset) > 0 and len(itemset) < len(current_t_items):
                        # find set of current transaction items in a model
                        model_indices = np.where(model['x']==itemset)
                        if model_indices[0].size > 0:
                            for model_i in range(0, len(model_indices[0])):
                                recommendations += model['y'][model_indices[0][model_i]].tolist()
                                recommendations += [x for x in model['x'][model_indices[0][model_i]].tolist() if x not in itemset]

                            recommendations = list(set(recommendations))
                            try:
                                recommendations.remove(0)
                            except ValueError:
                                pass

                    # if there's no enough recommendations, use the one from the recommender's list of recommendations
                    if len(recommendations) < self.get_k():
                        free_recomm_slots = self.get_k() - len(recommendations)
                        try:
                            for item_id in self.recommendations[current_user]['recommended']:
                                if item_id not in recommendations:
                                    recommendations.append(item_id)
                                    if free_recomm_slots <= 0:
                                        break
                                    else:
                                        free_recomm_slots -= 1

                        except KeyError:
                            # print current_user
                            pass

                    # no possible recommendations for current user, just skip it!
                    if len(recommendations) == 0:
                        case_without_history_c += 1
                        continue

                    # TEST RECOMMENDATIONS
                    if current_t_item in recommendations[0:self.get_k()]:
                        confusion_matrix['tp'] += 1
                        if self.get_k() < model_items_total:
                            confusion_matrix['tn'] += model_items_total - len(recommendations[0:self.get_k()])
                        confusion_matrix['fp'] += len(recommendations[0:self.get_k()]) - 1
                    else:
                        confusion_matrix['fn'] += 1
                        if self.get_k() < model_items_total:
                            confusion_matrix['tn'] += model_items_total - len(recommendations[0:self.get_k()]) - 1
                        confusion_matrix['fp'] += len(recommendations[0:self.get_k()])
                    itemset += [current_t_item]

                # end transaction-wise operations HERE

                current_t_id = transaction['order']
                current_t_items = []

            current_user = transaction['user']
            current_t_items.append(transaction['item'])

        end = time.time()
        precision, recall, fallout, f1, specificity = self.get_confusion_metrics(confusion_matrix)

        out_data = [model_type, k, precision, recall, fallout, f1, specificity]
        out_string = ''
        for out_item in out_data:
            out_string += str(out_item) + '\t'
        print out_string

        # return
        if self.log is not None:
            self.log_basic_test_info(model_type, self.get_k(), case_without_history_c, t_items_c)
            self.log_time(start, end, confusion_matrix)

            self.log_confusion_matrix(confusion_matrix)
            self.log_confusion_metrics(precision, recall, fallout, f1, specificity)
            self.log.info('-' * 23)

    def get_items_total(self, model):
        flatten_model = list(set(model['x'].flatten().tolist() + model['y'].flatten().tolist()))
        if 0 in flatten_model:
            flatten_model = [x for x in flatten_model if x != 0]

        return len(flatten_model)
