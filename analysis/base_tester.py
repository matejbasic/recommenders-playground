import numpy as np

class BaseTester:

    def log_basic_test_info(self, model_type, k, case_without_history_c, t_items_c):
        self.log.info('MODEL: ' + model_type)
        self.log.info('K size: ' + str(k))
        self.log.info('# users without history: ' + str(case_without_history_c))
        self.log.info('# transactions items: ' + str(t_items_c))

    def log_confusion_matrix(self, confusion_matrix):
        self.log.info('CONFUSION MATRIX:')
        self.log.info('# TP(retrieved relevant): ' + str(confusion_matrix['tp']))
        self.log.info('# FP(retrieved irrelevant): ' + str(confusion_matrix['fp']))
        self.log.info('# FN(not retrieved relevant): ' + str(confusion_matrix['fn']))
        self.log.info('# TN(not retrieved irrelevant): ' + str(confusion_matrix['tn']))

    def log_confusion_metrics(self, precision, recall, fallout, f1, specificity):
        self.log.info('PRECISION: ' + str(precision))
        self.log.info('RECALL: ' + str(recall))
        self.log.info('FALLOUT: ' + str(fallout))
        self.log.info('SPECIFICITY: ' + str(specificity))
        self.log.info('F1: ' + str(f1))

    def log_time(self, start, end, confusion_matrix):
        self.log.info('time: ' + str(end - start))
        if confusion_matrix['tp'] != 0:
            self.log.info('time/positive recommendation: ' + str( (end - start) / confusion_matrix['tp'] ))
        else:
            self.log.info('time/positive recommendation: 0' )

    def get_confusion_metrics(self, confusion_matrix):
        try:
            precision = float(confusion_matrix['tp']) / (confusion_matrix['tp'] + confusion_matrix['fp'])
        except ZeroDivisionError:
            precision = 0

        try:
            recall = float(confusion_matrix['tp']) / (confusion_matrix['tp'] + confusion_matrix['fn'])
        except ZeroDivisionError:
            recall = 0

        try:
            fallout = float(confusion_matrix['fp']) / (confusion_matrix['fp'] + confusion_matrix['tn'])
        except ZeroDivisionError:
            fallout = 0

        try:
            specificity = float(confusion_matrix['tn']) / (confusion_matrix['tn'] + confusion_matrix['fp'])
        except ZeroDivisionError:
            specificity = 0

        try:
            f1 = (2 * precision * recall) / (precision + recall)
        except ZeroDivisionError:
            f1 = 0

        return precision, recall, fallout, f1, specificity

    def set_k(self, k):
        try:
            self.k = int(k)
        except (ValueError, TypeError):
            self.k = -1
        return self.k

    def get_k(self):
        try:
            return self.k
        except AttributeError:
            self.set_k(None)
            return self.k

    def set_recommendations(self, recommendations = None):
        if type(recommendations) is dict:
            self.recommendations = recommendations
            return True
        else:
            self.recommendations = None
            return False

    def set_items_total_num(self, items_num = 0):
        if type(items_num) is int:
            self.items_total_num = items_num
            return True
        else:
            return False

    def set_test_data(self, transactions = None):
        if type(transactions) is list:
            self.transactions = transactions
            return True
        else:
            return False

    def set_logger(self, logger = None):
        if logger is not None:
            self.log = logger
