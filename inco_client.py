from dataclasses import dataclass

from datastore import Datastore

SUCCESS_STATUS_CODE = 0
KEY_EXIST = 100
ENTRY_DOES_NOT_EXIST = 101
KEY_DOES_NOT_EXIST = 102


@dataclass
class ClientResponse:
    status_codes: int = SUCCESS_STATUS_CODE
    msg: str = ''
    data: str = None


class HomeTask_Client:
    def __init__(self, store: Datastore):
        self._store: Datastore = store

    def insert(self, key: str, value: str) -> ClientResponse:
        try:
            self._store.get(key)
            return ClientResponse(status_codes=KEY_EXIST, msg='Key exist')
        except KeyError:
            pass

        self._store.set(key, value)
        return ClientResponse(status_codes=SUCCESS_STATUS_CODE, msg='Entry was successfully created')

    def select(self, key: str) -> ClientResponse:
        try:
            value = self._store.get(key)
            return ClientResponse(status_codes=SUCCESS_STATUS_CODE, msg='Entry retrieved successfully', data=value)
        except KeyError:
            return ClientResponse(status_codes=ENTRY_DOES_NOT_EXIST, msg='Entry does not exist')

    def update(self, key: str, value: str) -> ClientResponse:
        try:
            self._store.get(key)
            self._store.set(key, value)
            return ClientResponse(status_codes=SUCCESS_STATUS_CODE, msg='Entry was successfully updated')
        except KeyError:
            return ClientResponse(status_codes=KEY_DOES_NOT_EXIST, msg='Key does not exist')

    def delete(self, key: str) -> ClientResponse:
        try:
            self._store.get(key)
        except KeyError:
            return ClientResponse(status_codes=ENTRY_DOES_NOT_EXIST, msg='Entry does not exist')

        self._store.delete(key)
        return ClientResponse(status_codes=SUCCESS_STATUS_CODE, msg='Entry deleted successfully')
