import logging
import os


def get_consensus_logger(id: int):
    return get_logger(id, "consensus-node-")


def get_logger(id: int, name: str):
    logger = logging.getLogger(name+str(id))
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s %(filename)s [line:%(lineno)d] %(funcName)s %(levelname)s %(message)s ')
    if 'log' not in os.listdir(os.getcwd()):
        os.mkdir(os.getcwd() + '/log')
    full_path = os.path.realpath(os.getcwd()) + '/log/' + name+str(id) + ".log"
    file_handler = logging.FileHandler(full_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def bootstrap_log(fun):
    def prepare_bootstrap(*args, **kwargs):
        self = args[0]
        print('bootstrapping...')
        self.logger.info('node id %d is inserting dummy payload TXs' % (self.id))
        fun(*args, **kwargs)
        self.logger.info('node id %d completed the loading of dummy TXs' % (self.id))
        print(f'done bootstrapping... [{self.id}]')
    return prepare_bootstrap


def server_log(fun):
    def run(*args, **kwargs):
        self = args[0]
        pid = os.getpid()
        self.logger.info('node id %d is running on pid %d' % (self.id, pid))
        fun(*args, **kwargs)
        self.logger.info(
            'node %d\'s socket server finished listening on process id %d' % (self.id, pid))

    return run
