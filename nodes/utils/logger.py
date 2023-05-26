import logging
import os


def get_consensus_logger(id: int):
    logger = logging.getLogger("consensus-node-"+str(id))
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s %(filename)s [line:%(lineno)d] %(funcName)s %(levelname)s %(message)s ')
    if 'log' not in os.listdir(os.getcwd()):
        os.mkdir(os.getcwd() + '/log')
    full_path = os.path.realpath(os.getcwd()) + '/log/' + "consensus-node-"+str(id) + ".log"
    file_handler = logging.FileHandler(full_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def bootstrap_log(fun):
    def wrapper(*args, **kwargs):
        self = args[0]
        self.logger.info('node id %d is inserting dummy payload TXs' % (self.id))
        fun(*args, **kwargs)
        self.logger.info('node id %d completed the loading of dummy TXs' % (self.id))

    return wrapper
