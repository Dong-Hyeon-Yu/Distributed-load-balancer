from abc import ABC, abstractmethod


class Runnable(ABC):

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def prepare_bootstrap(self):
        pass

