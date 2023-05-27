from gevent import time, monkey;monkey.patch_all(thread=False)

from typing import Callable
import os
from BFTs.dispersedledger.core.bc_mvba import BM
from BFTs.dispersedledger.core.recover import RECOVER
from multiprocessing import Value as mpValue
from nodes.utils.key_loader import load_key
from nodes.Runnable import Runnable
from nodes.utils.logger import bootstrap_log


class DL2Node(BM, Runnable):

    def __init__(self, sid, id, S, Bfast, Bacs, N, f,
                 bft_from_server1: Callable, bft_to_client1: Callable,bft_from_server2: Callable, bft_to_client2: Callable, ready: mpValue, stop: mpValue, K=3, mode='debug', mute=False, tx_buffer=None):
        self.sPK, self.sPK1, self.sPK2s, self.ePK, self.sSK, self.sSK1, self.sSK2, self.eSK = load_key(id, N)
        #self.recv_queue = recv_q
        #self.send_queue = send_q
        self.bft_to_client1 = bft_to_client1
        self.bft_from_server1 = bft_from_server1

        self.bft_to_client2 = bft_to_client2
        self.bft_from_server2 = bft_from_server2
        self.ready = ready
        self.stop = stop
        self.mode = mode
        BM.__init__(self, sid, id, max(int(Bfast), 1), N, f,
                       self.sPK, self.sSK, self.sPK1, self.sSK1, self.sPK2s, self.sSK2,
                       send1=None, send2=None, recv=None, K=K, mute=mute)

        # Hotstuff.__init__(self, sid, id, max(S, 200), max(int(Bfast), 1), N, f, self.sPK, self.sSK, self.sPK1, self.sSK1, self.sPK2s, self.sSK2, self.ePK, self.eSK, send=None, recv=None, K=K, mute=mute)

    @bootstrap_log
    def prepare_bootstrap(self):
        if self.mode == 'test' or 'debug': #K * max(Bfast * S, Bacs)
            for r in range(max(self.K, 1)):
                self.transaction_buffer.bootstrap(self.B, 250)
                self.logger.info(f'node id {self.id} just inserts {self.B} TXs (total: {self.transaction_buffer.size()})')
        else:
            pass

    def run(self):

        pid = os.getpid()
        self.logger.info('node %d\'s starts to run consensus on process id %d' % (self.id, pid))

        self._send1 = lambda j, o: self.bft_to_client1((j, o))
        self._recv = lambda: self.bft_from_server1()
        self._send2 = lambda j, o: self.bft_to_client2((j, o))
        recv2 = lambda: self.bft_from_server2()

        self.prepare_bootstrap()

        while not self.ready.value:
            time.sleep(1)
            #gevent.sleep(1)

        recover = RECOVER(self.sid, self.id, self.B, self.N, self.f,
                         self.sPK, self.sSK, self.sPK1, self.sSK1, self.sPK2s, self.sSK2,
                         recv=recv2, K=self.K, mute=self.mute,logger=self.logger)

        recover.start()
        self.run_bft()

        recover.join()

        self.stop.value = True
