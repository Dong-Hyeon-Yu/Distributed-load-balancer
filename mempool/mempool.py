from gevent import monkey; monkey.patch_all(thread=False)

from nodes.utils.logger import get_logger

from ctypes import c_bool
from enum import IntEnum
from multiprocessing import Process, Value
from multiprocessing.connection import Connection
from typing import List, Callable

from mempool.storage.base_tx_storage import BaseTxStorage
from nodes.utils.workload_generator import zipfian_coefficient


class MsgType(IntEnum):
    SIZE = 0
    STORE_TX_BATCH = 1
    FETCH_TX_BATCH = 2
    COMMIT_BLOCK = 3
    BOOTSTRAP = 4


class Mempool(Process):

    def __init__(self, id, tx_storage: BaseTxStorage, recv_from_bft: Connection, send_to_bft: Connection,
                 mempool_ready: Value(c_bool), bft_stop: Value(c_bool), send_to_lb: Connection = None,
                 recv_from_lb: Connection = None):
        self.id = id
        self.ready = mempool_ready
        self.tx_storage: BaseTxStorage = tx_storage
        self.send_to_bft:   Connection = send_to_bft
        self.recv_from_bft: Connection = recv_from_bft
        self.send_to_lb:    Connection = send_to_lb
        self.recv_from_lb:  Connection = recv_from_lb
        self.bft_stop: Value = bft_stop
        self.logger = get_logger(id, "mempool-")
        super().__init__()

    def run(self):
        self.logger.info(f'node {self.id} is running..')
        print("running mempool...", flush=True)
        with self.ready.get_lock():
            self.ready.value = True
        self._listen(self.recv_from_bft, self.send_to_bft)

    def _listen(self, in_conn: Connection, out_conn: Connection):
        while not self.bft_stop.value:
            if in_conn.poll():
                tag, o = in_conn.recv()
                if tag == MsgType.SIZE:
                    self._size(out_conn)
                elif tag == MsgType.STORE_TX_BATCH:
                    self._store_tx_batch(o)
                elif tag == MsgType.FETCH_TX_BATCH:
                    self._fetch_tx_batch(out_conn, o)
                elif tag == MsgType.COMMIT_BLOCK:
                    self._commit_block(out_conn, o)
                elif tag == MsgType.BOOTSTRAP:
                    self._bootstrap_(out_conn, *o)

    def _listen_bft(self):
        pass

    def _listen_lb(self):
        pass

    def _size(self, out_conn: Connection):
        out_conn.send(self.tx_storage.size())

    def _store_tx_batch(self, tx_batch: List):
        self.tx_storage.store_tx_batch(tx_batch)

    def _fetch_tx_batch(self, out_conn: Connection, batch_size: int):
        batch = self.tx_storage.fetch_tx_batch(batch_size)
        self.logger.info(f"fetch tx batch size: {len(batch)}")
        out_conn.send(batch)

    def _commit_block(self, out_conn: Connection, tx_batch: List):
        self.logger.info(f'call commit_block with size {len(tx_batch)}')
        decoded_block = self.tx_storage.decode_block(tx_batch)
        out_conn.send(decoded_block)
        self.tx_storage.remove_committed_tx(decoded_block)

    def _bootstrap_(self, out_conn: Connection, node_id, batch_size, epoch, the_number_of_nodes, tx_size=250,
                   unbalanced_workload=False, dist_func: Callable = zipfian_coefficient, *args):

        self.tx_storage.bootstrap(node_id, batch_size, epoch, the_number_of_nodes, tx_size, unbalanced_workload,
                                  dist_func, *args)
        out_conn.send(True)
        self.logger.info(f"finish bootstrapping! {self.tx_storage.size()}")
