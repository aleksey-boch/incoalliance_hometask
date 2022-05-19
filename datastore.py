from abc import ABC, abstractmethod


class Datastore(ABC):

    @abstractmethod
    def set(self, key: str, value: str):
        pass

    @abstractmethod
    def get(self, key) -> str:
        pass

    @abstractmethod
    def delete(self, key: str):
        pass

    @abstractmethod
    def get_all_keys(self) -> list:
        pass
