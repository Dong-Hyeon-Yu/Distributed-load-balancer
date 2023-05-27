from gevent import monkey;monkey.patch_all(thread=False)

from typing import Callable
import os
from gevent import time
from BFTs.bdtbft.core.rotatinghotstuff import RotatingLeaderHotstuff
from nodes.utils.logger import bootstrap_log
from nodes.utils.key_loader import load_key
from nodes.Runnable import Runnable
from multiprocessing import Value as mpValue
from ctypes import c_bool


class RotatingHotstuffBFTNode (RotatingLeaderHotstuff, Runnable):

    def __init__(self, sid, id, S, T, Bfast, Bacs, N, f, bft_from_server: Callable, bft_to_client: Callable,
                 ready: mpValue, stop: mpValue, K=3, mode='debug', mute=False, bft_running: mpValue=mpValue(c_bool, True),
                 omitfast=False, unbalanced_workload=False):
        self.sPK, self.sPK1, self.sPK2s, self.ePK, self.sSK, self.sSK1, self.sSK2, self.eSK = load_key(id, N)
        self.send = lambda j, o: bft_to_client((j, o))
        self.recv = lambda: bft_from_server()
        self.ready = ready
        self.stop = stop
        self.mode = mode
        self.running = bft_running
        self.unbalanced_workload = unbalanced_workload

        Runnable.__init__(self, id=id, N=N, send=self.send, recv=self.recv)
        RotatingLeaderHotstuff.__init__(self, sid, id, S, T, max(int(Bfast), 1), max(int(Bacs/N), 1), N, f,
                                        self.sPK, self.sSK, self.sPK1, self.sSK1, self.sPK2s, self.sSK2, self.ePK, self.eSK,
                                        send=self.send, recv=self.recv, K=K, mute=mute, omitfast=omitfast)

    @bootstrap_log
    def prepare_bootstrap(self):
        if self.mode == 'test' or 'debug': #K * max(Bfast * S, Bacs)
            n = self.transaction_buffer.bootstrap(self.id,
                                              min(self.SLOTS_NUM * self.FAST_BATCH_SIZE, self.FALLBACK_BATCH_SIZE),
                                              self.K, self.N, 250, self.unbalanced_workload)
            self.logger.info(f'node id {self.id} just inserts {n} TXs (total: {self.transaction_buffer.size()})')
        else:
            pass

    def run(self):

        pid = os.getpid()
        self.logger.info('node %d\'s starts to run consensus on process id %d' % (self.id, pid))
        self.logger.info('parameters: N=%d, f=%d, S=%d, T=%d, fast-batch=%d, acs-batch=%d, K=%d, O=%d'
                         % (self.N, self.f, self.SLOTS_NUM, self.TIMEOUT, self.FAST_BATCH_SIZE, self.FALLBACK_BATCH_SIZE, self.K, self.omitfast))

        self.synchronize_bootstrap_among_nodes()

        while not self.ready.value:
            time.sleep(1)

        self.running.value = True
        self.run_bft()
        self.stop.value = True


