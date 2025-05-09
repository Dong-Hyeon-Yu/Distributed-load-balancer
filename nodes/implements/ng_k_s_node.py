import gevent
from coincurve import PrivateKey, PublicKey
# from fastecdsa import keys, curve
# from fastecdsa.point import Point
from gevent import monkey, time;

from BFTs.dumbong.core.ng_k_s import Dumbo_NG_k_s

monkey.patch_all(thread=False)

from typing import Callable
import os
from nodes.utils.make_random_tx import tx_generator
from multiprocessing import Value as mpValue, Queue as mpQueue, Process
from nodes.utils.key_loader import load_key


class NGSNode(Dumbo_NG_k_s):

    def __init__(self, sid, id, S, Bfast, Bacs, N, f,
                 bft_from_server: Callable, bft_to_client: Callable, ready: mpValue, stop: mpValue, mode='debug',
                 mute=False, tx_buffer=None, countpoint=0):
        self.sPK, self.sPK1, self.sPK2s, self.ePK, self.sSK, self.sSK1, self.sSK2, self.eSK = load_key(id, N)
        self.bft_from_server = bft_from_server
        self.bft_to_client = bft_to_client
        self.ready = ready
        self.stop = stop
        self.mode = mode
        self.flag = 0
        self.countpoint = countpoint
        if N == 4:
            K = 4
        elif N == 16:
            K = 2
        else:
            K = 1
        Dumbo_NG_k_s.__init__(self, sid, id, max(S, 10), max(int(Bfast), 1), N, f,
                              self.sPK, self.sSK, self.sPK1, self.sSK1, self.sPK2s, self.sSK2, self.ePK, self.eSK,
                              send=None, recv=None, K=K, countpoint=countpoint, mute=mute)

        # Hotstuff.__init__(self, sid, id, max(S, 200), max(int(Bfast), 1), N, f, self.sPK, self.sSK, self.sPK1, self.sSK1, self.sPK2s, self.sSK2, self.ePK, self.eSK, send=None, recv=None, K=K, mute=mute)

        self.initial = [0] * self.K
        # Hotstuff.__init__(self, sid, id, max(S, 200), max(int(Bfast), 1), N, f, self.sPK, self.sSK, self.sPK1, self.sSK1, self.sPK2s, self.sSK2, self.ePK, self.eSK, send=None, recv=None, K=K, mute=mute)

    def client(self, k):
        if os.getpid() == self.op:
            return
        itr = 0
        rnd_tx = tx_generator(250)
        suffix = hex(self.id) + hex(k) + ">"
        for r in range(max(self.SLOTS_NUM, 1)):
            suffix1 = hex(r) + suffix
            tx_s = f'{rnd_tx[:-len(suffix1)]}{suffix1}'
            tx = [tx_s for _ in range(self.B)]
            Dumbo_NG_k_s.submit_tx(self, tx, k)

        self.initial[k] = 1
        while True:
            gevent.sleep(0.2)
            suffix1 = hex(itr) + suffix
            buffer_len = Dumbo_NG_k_s.buffer_size(self, k)
            if buffer_len < 10:
                for r in range(max(1, 1)):
                    suffix2 = hex(r) + suffix1
                    tx_s = f'{rnd_tx[:-len(suffix2)]}{suffix2}'
                    tx = [tx_s for _ in range(self.B)]
                    # tx = rnd_tx[:-len(suffix2)] + suffix2
                    # batch.append(tx)
                    Dumbo_NG_k_s.submit_tx(self, tx, k)
            else:
                continue
            itr += 1

    def run(self):

        # self.logger.info('node %d\'s starts to run consensus on process id %d' % (self.id, self.op))

        self._send = lambda j, o: self.bft_to_client((j, o))
        self._recv = lambda: self.bft_from_server()

        # print("initial tx loading")

        client_threads = [gevent.spawn(self.client, k) for k in range(self.K)]

        while sum(self.initial) == self.K:
            gevent.sleep(1)
        # print("initial tx loaded")

        while not self.ready.value:
            time.sleep(1)
            # gevent.sleep(1)

        self.run_bft()
        gevent.joinall(client_threads)

        self.stop.value = True
