from gevent import monkey;monkey.patch_all(thread=False)
from multiprocessing.connection import Connection
from typing import List, Callable

from mempool.mempool import MsgType
from nodes.utils.workload_generator import zipfian_coefficient


class MempoolClient:

    def __init__(self, recv: Connection, send: Connection):
        self.out_conn: Connection = send
        self.in_conn:  Connection = recv

    def bootstrap(self, node_id, batch_size, epoch, the_number_of_nodes, tx_size=250, unbalanced_workload=False,
                  dist_func: Callable = zipfian_coefficient, *args):
        self.out_conn.send(
            (MsgType.BOOTSTRAP,
             (node_id, batch_size, epoch, the_number_of_nodes, tx_size, unbalanced_workload, dist_func, args)))
        while not self.in_conn.poll(1):
            pass
        assert self.in_conn.recv()  # expect True

    def fetch_tx_batch(self, batch_size) -> List:
        self.out_conn.send((MsgType.FETCH_TX_BATCH, batch_size))
        while not self.in_conn.poll(1):
            pass
        return self.in_conn.recv()

    def store_tx_batch(self, tx_batch: List):
        self.out_conn.send((MsgType.STORE_TX_BATCH, tx_batch))

    def size(self) -> int:
        self.out_conn.send((MsgType.SIZE, ''))
        while not self.in_conn.poll(1):
            pass
        return self.in_conn.recv()

    @property
    def epoch(self) -> int:
        self.out_conn.send((MsgType.EPOCH, ''))
        while not self.in_conn.poll(1):
            pass
        return self.in_conn.recv()

    def remove_committed_tx_from_raw_block(self, raw_block: List) -> List:
        self.out_conn.send((MsgType.COMMIT_BLOCK, raw_block))
        while not self.in_conn.poll(1):
            pass
        return self.in_conn.recv()

