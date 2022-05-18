from dataclasses import dataclass

from datastore import Datastore

SUCCESS_STATUS_CODE = 0
KEY_EXIST = 100
ENTRY_DOES_NOT_EXIST = 101
KEY_DOES_NOT_EXIST = 102

TRANSACTION_ALREADY_STARTED = 200
TRANSACTION_NOT_STARTED = 201
TRANSACTION_COMMIT_ERROR = 202


@dataclass
class ClientResponse:
    status_codes: int = SUCCESS_STATUS_CODE
    msg: str = ''
    data: str = None


class HomeTask_Client:

    def __init__(self, store: Datastore):
        self._store: Datastore = store
        self._is_transaction_started: bool = False
        self._transaction_log: dict = {}
        self._transaction_errors: dict = {}

    def insert(self, key: str, value: str) -> ClientResponse:
        try:
            self._store.get(key)
            error = ClientResponse(status_codes=KEY_EXIST, msg='Key exist')
            self._transaction_errors.update({len(self._transaction_errors): error})
            return error
        except KeyError:
            pass

        if self._is_transaction_started:
            self._transaction_log.update({len(self._transaction_log): (self._store.delete, (key,))})
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
            old_value = self._store.get(key)
        except KeyError:
            error = ClientResponse(status_codes=KEY_DOES_NOT_EXIST, msg='Key does not exist')
            self._transaction_errors.update({len(self._transaction_errors): error})
            return error

        if self._is_transaction_started:
            self._transaction_log.update({len(self._transaction_log): (self._store.set, (key, old_value))})
        self._store.set(key, value)

        return ClientResponse(status_codes=SUCCESS_STATUS_CODE, msg='Entry was successfully updated')

    def delete(self, key: str) -> ClientResponse:
        try:
            old_value = self._store.get(key)
        except KeyError:
            error = ClientResponse(status_codes=ENTRY_DOES_NOT_EXIST, msg='Entry does not exist')
            self._transaction_errors.update({len(self._transaction_errors): error})
            return error

        if self._is_transaction_started:
            self._transaction_log.update({len(self._transaction_log): (self._store.set, (key, old_value))})
        self._store.delete(key)

        return ClientResponse(status_codes=SUCCESS_STATUS_CODE, msg='Entry deleted successfully')

    def begin(self):
        if self._is_transaction_started:
            return ClientResponse(status_codes=TRANSACTION_ALREADY_STARTED, msg='Transaction already stared')

        self._is_transaction_started = True
        return ClientResponse(status_codes=SUCCESS_STATUS_CODE, msg='Transaction stared')

    def commit(self):
        if not self._is_transaction_started:
            return ClientResponse(status_codes=TRANSACTION_NOT_STARTED, msg='Transaction not stared')

        if self._transaction_errors:
            return ClientResponse(status_codes=TRANSACTION_COMMIT_ERROR, msg='Transaction has error')

        self._is_transaction_started = False
        self._transaction_log.clear()

    def rollback(self):
        self._is_transaction_started = False
        for _, event in sorted(self._transaction_log.items(), reverse=True):
            action, arg = event
            action(*arg)

        self._transaction_log.clear()

    def keys(self, pattern):
        pass
