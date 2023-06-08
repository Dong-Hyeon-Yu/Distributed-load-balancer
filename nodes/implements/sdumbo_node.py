from mempool.mempool_client import MempoolClient
from nodes.utils.logger import bootstrap_log
from gevent import monkey;monkey.patch_all(thread=False)

from typing import Callable
import os
from gevent import time
from BFTs.speedydumbobft.core.speedydumbo_s import SpeedyDumbo
from multiprocessing import Value as mpValue
from nodes.utils.key_loader import load_key
from nodes.Runnable import Runnable


class SDumboBFTNode (SpeedyDumbo, Runnable):

    def __init__(self, sid, id, B, N, f, bft_from_server: Callable, bft_to_client: Callable, ready: mpValue,
                 stop: mpValue, tx_storage: MempoolClient, K=3, mode='debug', mute=False, debug=False, unbalanced_workload=False):
        self.sPK, self.sPK1, self.sPK2s, self.ePK, self.sSK, self.sSK1, self.sSK2, self.eSK = load_key(id, N)
        self.send = lambda j, o: bft_to_client((j, o))
        self.recv = lambda: bft_from_server()
        self.ready = ready
        self.stop = stop
        self.mode = mode
        self.unbalanced_workload = unbalanced_workload

        Runnable.__init__(self, id=id, N=N, send=self.send, recv=self.recv)
        SpeedyDumbo.__init__(self, sid, id, max(int(B/N), 1), N, f,
                             self.sPK, self.sSK, self.sPK1, self.sSK1, self.sPK2s, self.sSK2, self.ePK, self.eSK,
                             self.send, self.recv, tx_storage=tx_storage, K=K, mute=mute, debug=debug)

    @bootstrap_log
    def prepare_bootstrap(self):
        if self.mode == 'test' or 'debug':
            self.transaction_buffer.bootstrap(self.id, self.B, self.K, self.N, 250, self.unbalanced_workload)
            self.logger.info(f'node id {self.id} just inserts TXs (total: {self.transaction_buffer.size()})')
        else:
            pass

    def run(self):

        pid = os.getpid()
        self.logger.info('node %d\'s starts to run consensus on process id %d' % (self.id, pid))

        self.synchronize_bootstrap_among_nodes()

        while not self.ready.value:
            time.sleep(1)
            #gevent.sleep(1)

        self.run_bft()
        self.stop.value = True
