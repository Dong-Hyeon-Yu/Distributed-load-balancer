from gevent import monkey;monkey.patch_all(thread=False)
from typing import Callable
import os
from gevent import time
from BFTs.bdtbft.core.bdt import Bdt
from nodes.utils.logger import bootstrap_log
from nodes.utils.key_loader import load_key
from nodes.Runnable import Runnable
from multiprocessing import Value as mpValue
from ctypes import c_bool


class BdtBFTNode (Bdt, Runnable):

    def __init__(self, sid, id, S, T, Bfast, Bacs, N, f, bft_from_server: Callable, bft_to_client: Callable, ready: mpValue, stop: mpValue, K=3, mode='debug', mute=False, bft_running: mpValue=mpValue(c_bool, False), omitfast=False):
        self.sPK, self.sPK1, self.sPK2s, self.ePK, self.sSK, self.sSK1, self.sSK2, self.eSK = load_key(id, N)
        self.bft_from_server = bft_from_server
        self.bft_to_client = bft_to_client
        self.send = lambda j, o: self.bft_to_client((j, o))
        self.recv = lambda: self.bft_from_server()
        self.ready = ready
        self.stop = stop
        self.mode = mode
        self.running = bft_running

        Bdt.__init__(self, sid, id, S, T, max(int(Bfast), 1), max(int(Bacs/N), 1), N, f, self.sPK, self.sSK, self.sPK1, self.sSK1, self.sPK2s, self.sSK2, self.ePK, self.eSK, send=self.send, recv=self.recv, K=K, mute=mute, omitfast=omitfast)

    @bootstrap_log
    def prepare_bootstrap(self):
        if self.mode == 'test' or 'debug': #K * max(Bfast * S, Bacs)
            for _ in range(self.K + 1):
                self.transaction_buffer.bootstrap(self.SLOTS_NUM, 250)
                self.logger.info(f'node id {self.id} just inserts {self.SLOTS_NUM} TXs (total: {self.transaction_buffer.size()})')
        else:
            pass

    def run(self):
        pid = os.getpid()
        self.logger.info('node %d\'s starts to run consensus on process id %d' % (self.id, pid))
        self.logger.info('parameters: N=%d, f=%d, S=%d, T=%d, fast-batch=%d, acs-batch=%d, K=%d, O=%d' % (self.N, self.f, self.SLOTS_NUM, self.TIMEOUT, self.FAST_BATCH_SIZE, self.FALLBACK_BATCH_SIZE, self.K, self.omitfast))

        self.prepare_bootstrap()

        while not self.ready.value:
            time.sleep(1)

        self.running.value = True

        self.run_bft()
        self.stop.value = True

