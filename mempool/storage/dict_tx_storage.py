import json
from typing import List, Callable

from mempool.data.transaction import Transaction
from mempool.storage.base_tx_storage import BaseTxStorage
from mempool.storage.gevent_support import gevent_support
from nodes.utils.workload_generator import zipfian_coefficient


class DictTxStorage(BaseTxStorage):

    """
    DictTxStorage manages a transaction storage as a dictionary which is thread-safe.
    """

    def __init__(self):
        self.storage: dict = dict()

    def _bootstrap_balanced_workload(self, batch_size, tx_size) -> int:
        tx_list = [Transaction.dummy(tx_size) for _ in range(batch_size)]
        self.store_tx_batch(tx_list)
        return len(tx_list)

    def _bootstrap_unbalanced_workload(self, node_id, batch_size, epoch, the_number_of_nodes, tx_size,
                                       dist_func: Callable = zipfian_coefficient, *args) -> int:
        total_tx = batch_size * epoch * the_number_of_nodes
        modified_total_tx = round(total_tx * dist_func(node_id, the_number_of_nodes))
        tx_list = [Transaction.dummy(tx_size) for _ in range(modified_total_tx)]
        self.store_tx_batch(tx_list)
        return len(tx_list)

    def fetch_tx_batch(self, batch_size) -> List[Transaction]:
        tx_batch = []
        while len(self.storage) > 0 and len(tx_batch) < batch_size:
            tx_batch.append(self.fetch_tx())
        return tx_batch

    def fetch_tx(self) -> Transaction:
        """this function does not guarantee the error when the storage is empty"""
        return self.storage.popitem()[1]

    def store_tx_batch(self, tx_batch: List[Transaction]):
        for tx in tx_batch:
            self.store_tx(tx)

    @gevent_support
    def store_tx(self, tx: Transaction):
        if self.storage.get(tx.hash) is None:
            self.storage[tx.hash] = tx

    def find_by_id(self, _id) -> Transaction:
        """return Transaction or None"""
        return self.storage.get(_id)

    def remove_by_id(self, _id: bytes) -> None:
        if self.storage.get(str(_id)):
            del self.storage[_id]

    def remove_all_by_id(self, _ids: List[bytes]) -> None:
        for _hash in _ids:
            self.remove_by_id(_hash)

    def size(self):
        return len(self.storage)

    def remove_committed_tx(self, block: List[Transaction]):
        for tx in block:
            self.remove_by_id(tx.hash)

    def remove_committed_tx_from_raw_block(self, raw_block: List) -> List:
        block = self._decode_block(raw_block)
        self.remove_committed_tx(block)
        return block

    def _decode_block(self, raw_block):
        block = set()
        for batch in raw_block:
            decoded_batch = json.loads(batch.decode(), object_hook=lambda tx: Transaction.from_json(tx))
            for tx in decoded_batch:
                block.add(tx)

        return list(block)