"""
Test (non-network) s3 functionality
"""
import unittest
from s3_manager import parse_path

PATH = "s3://some-bucket/some_path_a/some_path_b/some_destination"
PARSED = {
    "path": "s3://some-bucket/some_path_a/some_path_b/some_destination",
    "bucket": "some-bucket",
    "key": "some_path_a/some_path_b/some_destination",
}


class TestS3Manager(unittest.TestCase):
    """ Test (non-network) s3 functionality"""

    def test_parser(self):
        """ Check validity of path parser """
        parsed = parse_path(PATH)
        self.assertEqual(parsed["path"], PARSED["path"])
        self.assertEqual(parsed["bucket"], PARSED["bucket"])
        self.assertEqual(parsed["key"], PARSED["key"])
