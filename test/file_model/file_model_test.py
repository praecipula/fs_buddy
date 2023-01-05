import unittest
import io
from src.models.file import FileLikeObject

class TestFileLikeObject(unittest.TestCase):

    def test_create(self):
        self_file = FileLikeObject.create(__file__)
        assert(True)

