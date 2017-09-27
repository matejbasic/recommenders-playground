import time
from helpers.logger import *
from db.data_manager import DataManager
from recommenders.cbf import CbfRecommender
from recommenders.cfib import CfItemBasedRecommender
from recommenders.cfub import CfUserBasedRecommender
from recommenders.pi import PopularItemsRecommender
from analysis.tester import Tester

logger = get_logger()

start = time.time()
dm    = DataManager('config.json', 0.7)
iu    = dm.get_items_users('train')
uic   = dm.get_users_items_cats('train')
end   = time.time()

print 'data fetch:\t' + str(end - start)

tester = Tester()
tester.set_test_data(dm.get_orders('test'))
tester.set_logger(logger)

items_total_num = dm.get_items_total_num('all')

recommenders = [
    (CbfRecommender, 'CBF'),
    (CfUserBasedRecommender, 'CF User Based'),
    (CfItemBasedRecommender, 'CF Item Based'),
]

for recommender in recommenders:
    start = time.time()
    if recommender[1] is 'CBF':
        r = recommender[0](uic['cats'], uic['items'], uic['users'], uic['item_cats'], uic['user_items'])
    else:
        r = recommender[0](iu['items'], iu['users'], iu['items_users'], uic['user_items'])

    model = r.train()
    if recommender[1] is 'CBF' or 'Popular Items':
        tester.set_recommendations(r.generate_recommendations(k = 100))
    else:
        tester.set_recommendations(r.generate_recommendations(k = 100))
    end = time.time()
    logger.info(recommender[1] + ' model generation time: ' + str(end - start))
    print 'model generation time:\t' + str(end - start)

    for k in [1, 2, 3, 4, 5, 10, 15, 20, 30, 40, 50, 75, 100]:
        start = time.time()
        tester.test(k, items_total_num, recommender[1])
        end = time.time()
        print 'recommendation testing time for k=' + str(k) + ':\t' + str(end - start)

    print ''
