from abc import ABC, abstractmethod
from typing import Set, List


class BaseTxStorage(ABC):

    @abstractmethod
    def fetch_tx_batch(self, batch_size):
        pass

    @abstractmethod
    def fetch_tx(self):
        pass

    @abstractmethod
    def store_tx_batch(self, tx_batch: List):
        pass

    @abstractmethod
    def store_tx(self, tx):
        pass

    @abstractmethod
    def find_by_hash(self, _hash):
        pass

    @abstractmethod
    def remove_by_hash(self, _hash):
        pass

    @abstractmethod
    def remove_all_by_hash(self, hashes: Set):
        pass

    @abstractmethod
    def size(self):
        pass