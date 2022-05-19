import unittest

import inco_client
from memory_datastore import MemoryDatastore


class TestMemoryStore(unittest.TestCase):

    def setUp(self) -> None:
        store = MemoryDatastore()
        self.client = inco_client.HomeTask_Client(store)
        self.client.insert(key='qaz', value='wsx')

    def test_insert(self):
        response = self.client.insert(key='foo', value='boo')
        self.assertEqual(response.status_codes, inco_client.SUCCESS_STATUS_CODE, response.msg)

    def test_insert_dublicate(self):
        response = self.client.insert(key='qaz', value='wsx')
        self.assertEqual(response.status_codes, inco_client.KEY_EXIST, response.msg)

    def test_select(self):
        response = self.client.select(key='qaz')
        self.assertEqual(response.status_codes, inco_client.SUCCESS_STATUS_CODE, response.msg)

    def test_select_negative(self):
        response = self.client.select(key='wsx')
        self.assertEqual(response.status_codes, inco_client.ENTRY_DOES_NOT_EXIST, response.msg)

    def test_update(self):
        response = self.client.update(key='qaz', value='boo')
        self.assertEqual(response.status_codes, inco_client.SUCCESS_STATUS_CODE, response.msg)

    def test_update_negative(self):
        response = self.client.update(key='wsx', value='boo')
        self.assertEqual(response.status_codes, inco_client.KEY_DOES_NOT_EXIST, response.msg)

    def test_delete(self):
        response = self.client.delete(key='qaz')
        self.assertEqual(response.status_codes, inco_client.SUCCESS_STATUS_CODE, response.msg)

    def test_transaction_delete(self):
        response = self.client.begin()

        response = self.client.delete(key='qaz')
        self.assertEqual(response.status_codes, inco_client.SUCCESS_STATUS_CODE, response.msg)

        response = self.client.rollback()

        response = self.client.select(key='qaz')
        self.assertEqual(response.status_codes, inco_client.SUCCESS_STATUS_CODE, response.msg)

    def test_transaction_insert(self):
        response = self.client.begin()

        response = self.client.insert(key='foo', value='boo')
        self.assertEqual(response.status_codes, inco_client.SUCCESS_STATUS_CODE, response.msg)

        response = self.client.rollback()

        response = self.client.select(key='foo')
        self.assertEqual(response.status_codes, inco_client.ENTRY_DOES_NOT_EXIST, response.msg)

    def test_transaction_delete_insert(self):
        response = self.client.begin()

        response = self.client.delete(key='qaz')
        self.assertEqual(response.status_codes, inco_client.SUCCESS_STATUS_CODE, response.msg)

        response = self.client.insert(key='foo', value='boo')
        self.assertEqual(response.status_codes, inco_client.SUCCESS_STATUS_CODE, response.msg)

        response = self.client.insert(key='fooboo', value='boo')
        self.assertEqual(response.status_codes, inco_client.SUCCESS_STATUS_CODE, response.msg)

        response = self.client.rollback()

        response = self.client.select(key='qaz')
        self.assertEqual(response.status_codes, inco_client.SUCCESS_STATUS_CODE, response.msg)

        response = self.client.select(key='foo')
        self.assertEqual(response.status_codes, inco_client.ENTRY_DOES_NOT_EXIST, response.msg)

    def test_keys_select(self):
        response = self.client.keys(pattern='q?z')
        self.assertEqual(response.status_codes, inco_client.SUCCESS_STATUS_CODE, response.msg)
        self.assertEqual(response.data, ['qaz', ])
