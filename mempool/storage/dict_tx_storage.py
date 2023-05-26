from typing import Set, List

from mempool.data.transaction import Transaction
from mempool.storage.base_tx_storage import BaseTxStorage


class DictTxStorage(BaseTxStorage):

    """
    DictTxStorage manages a transaction storage as a dictionary which is thread-safe.
    """

    def __init__(self):
        self.storage: dict = dict()

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

    def store_tx(self, tx: Transaction):
        if self.storage.get(tx.hash) is None:
            self.storage[tx.hash] = tx

    def find_by_hash(self, _hash) -> Transaction:
        """return Transaction or None"""
        return self.storage.get(_hash)

    def remove_by_hash(self, _hash: bytes) -> None:
        if self.storage.get(str(_hash)):
            del self.storage[_hash]

    def remove_all_by_hash(self, hashes: List[str]) -> None:
        for _hash in hashes:
            self.remove_by_hash(_hash)

    def size(self):
        return len(self.storage)
