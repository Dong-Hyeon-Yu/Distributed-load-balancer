from abc import ABC, abstractmethod
from typing import Set, List, Callable
from nodes.utils.workload_generator import zipfian_coefficient


class BaseTxStorage(ABC):

    def bootstrap(self, node_id, batch_size, epoch, the_number_of_nodes, tx_size=250, unbalanced_workload=False,
                  dist_func: Callable = zipfian_coefficient, *args) -> int:
        if unbalanced_workload:
            return self._bootstrap_unbalanced_workload(node_id, batch_size, epoch, the_number_of_nodes, tx_size, dist_func, *args)
        else:
            return self._bootstrap_balanced_workload(batch_size * epoch, tx_size)

    @abstractmethod
    def _bootstrap_balanced_workload(self, batch_size, tx_size) -> int:
        pass

    @abstractmethod
    def _bootstrap_unbalanced_workload(self, node_id, batch_size, epoch, the_number_of_nodes, tx_size,
                                       dist_func: Callable, *args) -> int:
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
