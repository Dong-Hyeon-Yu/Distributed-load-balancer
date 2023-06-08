from gevent import monkey;monkey.patch_all(thread=False)
from _queue import Empty
from enum import IntEnum
import random
from multiprocessing import Process, Value as mpValue
from typing import Callable

import gevent
from gevent.queue import Queue

from mempool.mempool_client import MempoolClient
from nodes.utils.logger import get_logger


class MsgTag(IntEnum):
    PROVE = 0
    MEMPOOL_INFO = 1
    PULL_REQ = 2
    LOAD = 3


class LoadBalancer(Process):

    """
    Load Balancer runs as a separated process not to disturb the consensus engine. In Python, multi-threading is
    critical to the performance because of GIL primitives of Python.
    """

    def __init__(self, id: int, N: int, sampling_size: int, batch_size: int, tx_storage: MempoolClient, bft_stop: mpValue,
                 ready: mpValue, client_lb_mpq, recv: Callable):
        super().__init__()
        self.id = id
        self.client_lb_mpq = client_lb_mpq
        self.node_list = [i for i in range(N) if i != id]
        self.timeout = 0.01
        self.sampling_size = sampling_size
        self.batch_size = batch_size
        self.threshold = batch_size * 2
        self.recently_selected_node = None
        self.ready = ready
        self.logger = get_logger(self.id, "load balancer-")

        # sockets shared with web client and server, respectively.
        self.send: Callable = self.client_lb_mpq.put_nowait
        self.recv = recv

        #
        self.received_msg = [Queue() for _ in range(MsgTag.__len__())]

        # shared variables with the bft node
        self.tx_storage: MempoolClient = tx_storage
        self.bft_stop = bft_stop  # bft consensus status

    def run(self) -> None:
        import os
        self.logger.info(f'node {self.id} is running.. on {os.getpid()}')
        try:  # Todo: gevent sleep
            gevent.joinall([
                gevent.spawn(self._request_load),
                gevent.spawn(self._listen_forever),
                gevent.spawn(self._response_mempool_info),
                gevent.spawn(self._response_to_load_request),
            ])
            self.ready.value = True
        except Exception as e:
            self.ready.value = False

    def _listen_forever(self):
        while not self.bft_stop.value:
            try:
                _, o = self.recv()
                self.received_msg[o[0]].put_nowait(o)
            except Empty:
                pass
            gevent.sleep()

    def _response_mempool_info(self):
        while not self.bft_stop.value:
            if not self.received_msg[MsgTag.PROVE].empty():
                tag, mempool_size, sender = self.received_msg[MsgTag.PROVE].get_nowait()
                self.logger.info(f"prove request: <-- node{sender}")
                self.send((sender, (MsgTag.MEMPOOL_INFO, self._load_balancing_effect(mempool_size), self.id)))
                self.logger.info(f'send lb_effect: --> node{sender} ')
            gevent.sleep()

    def _response_to_load_request(self):
        while not self.bft_stop.value:
            if not self.received_msg[MsgTag.PULL_REQ].empty():
                tag, _, sender = self.received_msg[MsgTag.PULL_REQ].get_nowait()
                self.logger.info(f"get pull request: <-- node{sender}")
                self.send((sender, (MsgTag.LOAD, self.tx_storage.fetch_tx_batch(self.batch_size), self.id)))
                self.logger.info(f'forward load: --> node{sender} ')
            gevent.sleep()

    def _request_load(self):
        while not self.bft_stop.value:
            if self.tx_storage.size() < self.threshold:
                self.logger.info('transaction starvation!!')
                candidates = self._sample_candidates()
                for candidate in candidates:
                    self.logger.info(f'request mempool info: --> node{candidate}')
                    self.send((candidate, (MsgTag.PROVE, self.tx_storage.size(), self.id)))

                target = None
                _epoch = self.tx_storage.epoch  # set timer
                self.logger.debug('set timer')
                while target is None and self.tx_storage.epoch - _epoch < 1:
                    if not self.received_msg[MsgTag.MEMPOOL_INFO].empty():
                        self.logger.debug('about to retrieve Mempool response from queue')
                        tag, lb_effect, candidate = self.received_msg[MsgTag.MEMPOOL_INFO].get_nowait()
                        target = candidate if lb_effect > 1 else None

                if target is not None:
                    self.send((target, (MsgTag.PULL_REQ, '', self.id)))

                    self.recently_selected_node = None
                    _epoch = self.tx_storage.epoch  # reset timer
                    while self.recently_selected_node is None:  # and self.tx_storage.epoch - _epoch < 2:
                        if not self.received_msg[MsgTag.LOAD].empty():
                            tag, microblock, _target = self.received_msg[MsgTag.LOAD].get_nowait()
                            if _target == target:
                                self.logger.info(f"finally get forwarded from node {target}")
                                self.tx_storage.store_tx_batch(microblock)
                                self.recently_selected_node = _target
            gevent.sleep()

    def _sample_candidates(self):
        """
            sampling nodes' ids to prove their mempool status for load-pulling
        """
        candidates = random.sample(population=self.node_list, k=self.sampling_size)
        if self.recently_selected_node \
            and self.recently_selected_node not in candidates:
            candidates = candidates[:self.sampling_size-1] + self.recently_selected_node

        return set(candidates)

    def _load_balancing_effect(self, your_mempool_size):
        return (self.tx_storage.size() - your_mempool_size) / self.batch_size - 1


