from typing import Callable
import os
from gevent import time
from BFTs.honeybadgerbft.core.honeybadger import HoneyBadgerBFT
from nodes.utils.logger import get_consensus_logger, bootstrap_log
from nodes.utils.make_random_tx import tx_generator
from nodes.utils.key_loader import load_key_
from nodes.Runnable import Runnable
from multiprocessing import Value as mpValue
from ctypes import c_bool


class HoneyBadgerBFTNode (HoneyBadgerBFT, Runnable):

    def __init__(self, sid, id, B, N, f, bft_from_server: Callable, bft_to_client: Callable, ready: mpValue, stop: mpValue, K=3, mode='debug', mute=False, bft_running: mpValue=mpValue(c_bool, False), tx_buffer=None):
        self.sPK, self.ePK, self.sSK, self.eSK = load_key_(id)
        self.bft_from_server = bft_from_server
        self.bft_to_client = bft_to_client
        self.send = lambda j, o: self.bft_to_client((j, o))
        self.recv = lambda: self.bft_from_server()
        self.ready = ready
        self.stop = stop
        self.mode = mode
        self.running = bft_running
        HoneyBadgerBFT.__init__(self, sid, id, B, N, f, self.sPK, self.sSK, self.ePK, self.eSK, send=self.send, recv=self.recv, K=K, mute=mute, logger=get_consensus_logger())

    @bootstrap_log
    def prepare_bootstrap(self):
        if self.mode == 'test' or 'debug':
            for r in range(self.K * self.B):
                tx = tx_generator(250) # Set each dummy TX to be 250 Byte
                HoneyBadgerBFT.submit_tx(self, tx)
        else:
            pass
            # TODO: submit transactions through tx_buffer

    def run(self):
        pid = os.getpid()
        self.logger.info('node %d\'s starts to run consensus on process id %d' % (self.id, pid))

        self.prepare_bootstrap()

        while not self.ready.value:
            time.sleep(1)

        self.running.value = True

        self.run_bft()
        self.stop.value = True

