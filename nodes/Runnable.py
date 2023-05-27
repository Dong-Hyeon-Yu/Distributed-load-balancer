import queue
from abc import ABC, abstractmethod
from enum import Enum
from typing import Callable

import gevent


# Todo: separate communication message queue
class Runnable(ABC):

    def __init__(self, id, N, send: Callable, recv: Callable):
        self.N = N
        self.id = id
        self.recv: Callable = recv
        self.send: Callable = send
        self.is_bootstrapped = [False] * N
        self.bootstrap_answer = [False] * N
        self.bft_ready = False

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def prepare_bootstrap(self):
        pass

    def synchronize_bootstrap_among_nodes(self):
        print("waiting for all nodes to finish bootstrapping...")
        gevent.joinall([
            gevent.spawn(self._prepare_bootstrap),
            gevent.spawn(self._wait_bootstrap),
            gevent.spawn(self._listen_bootstrap),
            gevent.spawn(self._check_bootstrap),
            ]
        )

        assert self.bft_ready
        print("finish bootstrapping...")

    def _prepare_bootstrap(self):
        self.prepare_bootstrap()
        self._finish_bootstrap()

    def _finish_bootstrap(self):
        self.is_bootstrapped[self.id] = True
        self.bootstrap_answer[self.id] = True

    def _check_bootstrap(self):
        # print("check!")
        # self.logger.debug("check!")
        while not self.bft_ready:
            for i in range(self.N):
                if not self.is_bootstrapped[i] and i != self.id:
                    self.send(i, (BootstrapMsg.BOOTSTRAP_CHECK, self.id))
            # print("checking!")
            # self.logger.debug("checking!")
            gevent.sleep(0.5)
            # print("check again!")
            # self.logger.debug("check again!")

    def _listen_bootstrap(self):
        # print("listen!")
        # self.logger.debug("listen!")
        while not self.bft_ready:
            # print("listen carefully!")
            # self.logger.debug("listen carefully!")
            try:
                _, (msg, sender) = self.recv()
            except queue.Empty:
                gevent.sleep(0.5)
                continue
            # print("get msg!")
            # self.logger.debug("get msg!")
            if msg == BootstrapMsg.BOOTSTRAP_YES:
                self.is_bootstrapped[sender] = True
            elif msg == BootstrapMsg.BOOTSTRAP_CHECK and not self.bootstrap_answer[sender]:
                if self.is_bootstrapped[self.id]:
                    self.send(sender, (BootstrapMsg.BOOTSTRAP_YES, self.id))
                    self.bootstrap_answer[sender] = True
                else:
                    self.send(sender, (BootstrapMsg.BOOTSTRAP_NO, self.id))

    def _wait_bootstrap(self):
        # print("wait!")
        # self.logger.debug("wait!")
        # gevent.spawn(self._listen_bootstrap())
        while not self.bft_ready:
            # print("wait again!")
            # self.logger.debug("wait again!")
            gevent.sleep(0.5)

            if all(self.is_bootstrapped) and all(self.bootstrap_answer):
                self.bft_ready = True


class BootstrapMsg(Enum):
    BOOTSTRAP_CHECK = 'bootstrap check'
    BOOTSTRAP_YES = 'finish bootstrapping'
    BOOTSTRAP_NO = 'not finish yet'
