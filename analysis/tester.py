import time
import numpy as np
from base_tester import BaseTester

class Tester(BaseTester):

    def __init__(self, recommendations = None, k = None):
        if recommendations is not None:
            self.set_recommendations(recommendations)
        if k is not None:
            self.set_k(k)

        self.log = None
        self.transactions = None

    def test(self, k = None, items_total_num = 0, model_type = ""):
        if self.recommendations is None or self.transactions is None:
            return False

        if k is not None:
            self.set_k(k)

        self.set_items_total_num(items_total_num)

        t_items_c               = 0
        case_without_history_c  = 0

        confusion_matrix = {'tp': 0, 'tn': 0, 'fp': 0, 'fn': 0}

        users_unused_recommendations = {}
        users_unused_items_num       = {}

        start = time.time()

        # testing
        for t_item in self.transactions:
            t_items_c += 1
            if t_item['user'] in self.recommendations.keys():
                if self.recommendations[t_item['user']]['recommended'] is None:
                    if self.recommendations[t_item['user']]['known_items'] is not None: # well, use the known items!
                        self.recommendations[t_item['user']]['recommended'] = self.recommendations[t_item['user']]['known_items']
                    else:
                        case_without_history_c += 1
                        continue

                # user's unused init
                if t_item['user'] not in users_unused_recommendations.keys():
                    users_unused_recommendations[t_item['user']] = self.recommendations[t_item['user']]['recommended'][0:self.get_k()]
                if t_item['user'] not in users_unused_items_num.keys():
                    users_unused_items_num[t_item['user']] = self.items_total_num

                # if an transaction item is recommended
                if t_item['item'] in self.recommendations[t_item['user']]['recommended'][0:self.get_k()]:
                    confusion_matrix['tp'] += 1
                    # remove an item from unused recommendations
                    if t_item['item'] in users_unused_recommendations[t_item['user']]:
                        users_unused_recommendations[t_item['user']].remove(t_item['item'])
                else:
                    confusion_matrix['fn'] += 1
                    users_unused_items_num[t_item['user']] -= 1
            else:
                # we're not able to predict anything!
                case_without_history_c += 1

        end = time.time()

        for user_id in users_unused_recommendations:
            confusion_matrix['fp'] += len(users_unused_recommendations[user_id])
            confusion_matrix['tn'] += users_unused_items_num[user_id]
            confusion_matrix['tn'] -= len(users_unused_recommendations[user_id])

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
