import logging

def get_logger(logger_name = 'recomm-tester', file_name = 'test.log'):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers = []

    fh = logging.FileHandler(file_name)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)

    return logger
