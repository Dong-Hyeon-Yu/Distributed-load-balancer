import json
from typing import Set, List, Callable
from gevent.queue import Queue
import string
import random

from mempool.storage.base_tx_storage import BaseTxStorage
from mempool.storage.gevent_support import gevent_support
from nodes.utils.workload_generator import zipfian_coefficient


def tx_generator(size=250, chars=string.ascii_uppercase + string.digits):
    return '<Dummy TX: ' + ''.join(random.choice(chars) for _ in range(size - 16)) + '>'


class QueueTxStorage(BaseTxStorage):

    def __init__(self):
        self.storage = Queue()

    def _bootstrap_balanced_workload(self, batch_size, tx_size=250) -> int:
        tx_list = [tx_generator(tx_size) for _ in range(batch_size)]
        self.store_tx_batch(tx_list)
        return len(tx_list)

    def _bootstrap_unbalanced_workload(self, node_id, batch_size, epoch, the_number_of_nodes, tx_size,
                                       dist_func: Callable = zipfian_coefficient, *args) -> int:
        total_tx = batch_size * epoch * the_number_of_nodes
        modified_total_tx = round(total_tx * dist_func(node_id, the_number_of_nodes))
        tx_list = [tx_generator(tx_size) for _ in range(modified_total_tx)]
        self.store_tx_batch(tx_list)
        return len(tx_list)

    def fetch_tx_batch(self, batch_size):
        tx_to_send = []
        while self.storage.qsize() > 0 and len(tx_to_send) < batch_size:
            tx_to_send.append(self.storage.get_nowait())
        return tx_to_send

    def fetch_tx(self):
        return self.storage.get_nowait()

    def store_tx_batch(self, tx_batch: List):
        for tx in tx_batch:
            self.store_tx(tx)

    @gevent_support
    def store_tx(self, tx):
        self.storage.put_nowait(tx)

    def find_by_id(self, _id):
        pass

    def remove_by_id(self, _id):
        pass

    def remove_all_by_id(self, _ids: Set):
        pass

    def size(self):
        return self.storage.qsize()

    def remove_committed_tx(self, block: List[str]):
        pass

    def remove_committed_tx_from_raw_block(self, raw_block: List[str]) -> List[str]:
        block = self._decode_block(raw_block)
        self.remove_committed_tx(block)
        return block

    # decode raw block to strings
    def _decode_block(self, raw_block):
        block = set()
        for batch in raw_block:
            decoded_batch = json.loads(batch.decode())
            for tx in decoded_batch:
                block.add(tx)

        return list(block)