from abc import ABC, abstractmethod


class Datastore(ABC):

    @abstractmethod
    def set(self, key, value):
        pass

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def delete(self, key):
        pass
