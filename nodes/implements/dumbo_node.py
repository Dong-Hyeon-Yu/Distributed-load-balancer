from mempool.storage.base_tx_storage import BaseTxStorage
from mempool.storage.dict_tx_storage import DictTxStorage
from mempool.storage.queue_tx_storage import QueueTxStorage
from nodes.utils.logger import bootstrap_log
from gevent import monkey;monkey.patch_all(thread=False)

from typing import Callable
import os
from gevent import time
from BFTs.dumbobft.core.dumbo import Dumbo
from nodes.utils.key_loader import load_key
from nodes.Runnable import Runnable
from multiprocessing import Value as mpValue
from ctypes import c_bool


class DumboBFTNode (Dumbo, Runnable):

    def __init__(self, sid, id, B, N, f, bft_from_server: Callable, bft_to_client: Callable, ready: mpValue,
                 stop: mpValue, K = 3, mode='debug', mute=False, debug=False, bft_running: mpValue = mpValue(c_bool, False),
                 tx_storage: BaseTxStorage = QueueTxStorage()):
        self.sPK, self.sPK1, self.sPK2s, self.ePK, self.sSK, self.sSK1, self.sSK2, self.eSK = load_key(id, N)
        self.bft_from_server = bft_from_server
        self.bft_to_client = bft_to_client
        self.send = lambda j, o: self.bft_to_client((j, o))
        self.recv = lambda: self.bft_from_server()
        self.ready = ready
        self.stop = stop
        self.mode = mode
        self.running = bft_running
        Dumbo.__init__(self, sid, id, max(int(B/N), 1), N, f, self.sPK, self.sSK, self.sPK1, self.sSK1, self.sPK2s,
                       self.sSK2, self.ePK, self.eSK, self.send, self.recv, K=K, mute=mute, debug=debug)
        self.transaction_buffer: BaseTxStorage = tx_storage

    # override Runnable
    @bootstrap_log
    def prepare_bootstrap(self):
        if self.mode == 'test' or 'debug': #K * max(Bfast * S, Bacs)
            for _ in range(self.K):
                self.transaction_buffer.bootstrap(self.B, 250)
                self.logger.info(f'node id {self.id} just inserts {self.B} TXs (total: {self.transaction_buffer.size()})')
        else:
            pass

    # override Runnable
    def run(self):
        pid = os.getpid()
        self.logger.info('node %d\'s starts to run consensus on process id %d' % (self.id, pid))

        self.prepare_bootstrap()

        while not self.ready.value:
            time.sleep(1)

        self.running.value = True

        self.run_bft()
        self.stop.value = True
