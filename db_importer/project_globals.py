import json

def get_dataset_dir():
    with open('../config.json') as config_data:
        config = json.load(config_data)
        [str(x) for x in config]
        return config['data']['dir']

    return None

def get_batch_size():
    with open('../config.json') as config_data:
        config = json.load(config_data)
        [str(x) for x in config]
        return config['data']['batch_size']

    return None
