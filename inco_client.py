import fnmatch
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

    def _set_action(self, action, *args):
        if self._is_transaction_started:
            self._transaction_log.update({len(self._transaction_log): (action, args)})
        else:
            action(*args)

    def _log_update(self, error):
        if self._is_transaction_started:
            self._transaction_errors.update({len(self._transaction_errors): error})
        return error

    def insert(self, key: str, value: str) -> ClientResponse:
        try:
            self._store.get(key)
            return self._log_update(error=ClientResponse(status_codes=KEY_EXIST, msg='Key exist'))
        except KeyError:
            pass

        self._set_action(self._store.set, key, value)
        return ClientResponse(msg='Entry was successfully created')

    def select(self, key: str) -> ClientResponse:
        try:
            value = self._store.get(key)
            return ClientResponse(msg='Entry retrieved successfully', data=value)
        except KeyError:
            return ClientResponse(status_codes=ENTRY_DOES_NOT_EXIST, msg='Entry does not exist')

    def update(self, key: str, value: str) -> ClientResponse:
        try:
            self._store.get(key)
        except KeyError:
            return self._log_update(error=ClientResponse(status_codes=KEY_DOES_NOT_EXIST, msg='Key does not exist'))

        self._set_action(self._store.set, key, value)
        return ClientResponse(msg='Entry was successfully updated')

    def delete(self, key: str) -> ClientResponse:
        try:
            self._store.get(key)
        except KeyError:
            return self._log_update(error=ClientResponse(status_codes=ENTRY_DOES_NOT_EXIST, msg='Entry does not exist'))

        self._set_action(self._store.delete, key)
        return ClientResponse(msg='Entry deleted successfully')

    def begin(self) -> ClientResponse:
        if self._is_transaction_started:
            return ClientResponse(status_codes=TRANSACTION_ALREADY_STARTED, msg='Transaction already stared')

        self._is_transaction_started = True
        self._transaction_log.clear()
        return ClientResponse(msg='Transaction stared')

    def commit(self) -> ClientResponse:
        if not self._is_transaction_started:
            return ClientResponse(status_codes=TRANSACTION_NOT_STARTED, msg='Transaction not stared')

        if self._transaction_errors:
            return ClientResponse(status_codes=TRANSACTION_COMMIT_ERROR, msg='Transaction has error')

        self._is_transaction_started = False
        for _, event in sorted(self._transaction_log.items(), reverse=True):
            action, arg = event
            action(*arg)

        self._transaction_log.clear()
        return ClientResponse(msg='Transaction was successfully commit')

    def rollback(self) -> ClientResponse:
        self._is_transaction_started = False
        self._transaction_log.clear()
        return ClientResponse(msg='Transaction was successfully rollback')

    def keys(self, pattern: str) -> ClientResponse:
        value = fnmatch.filter(self._store.get_all_keys(), pattern)
        return ClientResponse(msg='Keys retrieved successfully', data=value)
