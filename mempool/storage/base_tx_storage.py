from abc import ABC, abstractmethod
from typing import Set, List


class BaseTxStorage(ABC):

    @abstractmethod
    def bootstrap(self, batch_size, tx_size):
        pass

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
    def find_by_id(self, _id):
        pass

    @abstractmethod
    def remove_by_id(self, _id):
        pass

    @abstractmethod
    def remove_all_by_id(self, _ids: Set):
        pass

    @abstractmethod
    def size(self):
        pass

    @abstractmethod
    def remove_committed_tx(self, block: List):
        pass

    @abstractmethod
    def remove_committed_tx_from_raw_block(self, raw_block: List) -> List:
        """
        Deserialize the given serialized(raw) block.
        Then remove committed transactions from this storage,
        and return the decoded block
        """
        pass
