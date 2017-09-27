import time
from helpers.logger import *
from db.data_manager import DataManager
from recommenders.assoc import AssociationRecommender
from analysis.assoc_tester import AssociationTester

logger = get_logger()

start  = time.time()
dm     = DataManager('config.json', 0.7)
iu     = dm.get_items_users('train')
uic    = dm.get_users_items_cats('train')
orders = dm.get_orders('train')
end    = time.time()

print 'data fetch:\t' + str(end - start)

assoc = AssociationRecommender(orders, iu['items'], iu['users'], iu['items_users'], uic['user_items'])

tester = AssociationTester()
tester.set_test_data(dm.get_orders('test'))
tester.set_logger(logger)

supp = 0.0002
conf = 0.6

for alg in ['apriori', 'eclat', 'fpgrowth']:
    start = time.time()
    model           = assoc.train(min_support = supp, min_confidence = conf, algorithm = alg)
    recommendations = assoc.generate_recommendations(k = 100)
    end = time.time()
    logger.info('Association ' + alg + ' model generation time: ' + str(end - start))
    print 'model generation time:\t' + str(end - start)

    for k in [1, 2, 3, 4, 5, 10, 15, 20, 30, 40, 50, 75, 100]:
        start = time.time()
        tester.set_recommendations(recommendations)
        tester.test(model, k, alg)
        end = time.time()
        print 'recommendation testing time:\t' + str(end - start)

    print ''
