import csv
from project_globals import *

BATCH_SIZE  = get_batch_size()

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def iterate_data(file_name, do_row):
    with open(DATASET_DIR + file_name, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            do_row(row)
