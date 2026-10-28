"""Offset validation and credential checks of the Stopover server."""
import base64
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from stopover_server.broker import Broker
from stopover_server.partition import Partition, PartitionItem


class SetOffsetTest(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory(prefix='stopover-offsets-')
        self.addCleanup(self.directory.cleanup)
        self.partition = Partition('offsets', 0, self.directory.name, create_if_missing=True)
        self.addCleanup(lambda: self.partition._store.close()
                        if hasattr(self.partition._store, 'close') else None)
        for timestamp in range(3):  # indexes 0..2
            self.partition.put(PartitionItem(b'x', timestamp=timestamp))

    def test_offset_can_mark_everything_consumed(self):
        self.partition.set_offset('group', 2)
        self.assertEqual(self.partition._get_offset('group'), 2)
        self.assertIsNone(self.partition.get('group'))

    def test_offset_is_clamped_and_validated(self):
        self.partition.set_offset('group', 99)
        self.assertEqual(self.partition._get_offset('group'), 2)
        self.partition.set_offset('group', -1)
        self.assertEqual(self.partition.get('group')['index'], 0)
        for invalid in (-2, '1', 1.5, None, True):
            with self.assertRaises(ValueError):
                self.partition.set_offset('group', invalid)


class AuthTest(unittest.TestCase):
    def broker(self, auth):
        directory = TemporaryDirectory(prefix='stopover-auth-')
        self.addCleanup(directory.cleanup)
        config = {'global': {'data_dir': directory.name, 'partitions': 1,
                             'rebalance_interval': 3600, 'receiver_timeout': 60,
                             'prune_interval': 3600, 'ttl': 86400},
                  'streams': {}, 'auth': auth}
        with patch('stopover_server.broker.Thread.start'):
            return Broker(config)

    def test_credentials(self):
        broker = self.broker({'client': 'se:cret'})
        header = lambda pair: {'authorization': 'Basic ' + base64.b64encode(pair).decode()}
        self.assertTrue(broker.check_authorized(header(b'client:se:cret')))
        for pair in (b'client:wrong', b'other:se:cret', b'client:'):
            self.assertFalse(broker.check_authorized(header(pair)))


if __name__ == '__main__':
    unittest.main()
