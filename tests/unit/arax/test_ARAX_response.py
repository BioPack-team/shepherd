"""ARAX's own ARAXResponse tests (the unittest block from
RTXteam/RTX @ 9485431, code/ARAX/ARAXQuery/ARAX_response.py), run against the port.
"""

import unittest

from shepherd_utils.arax.ARAX_response import ARAXResponse


import unittest


class ResponseTests(unittest.TestCase):

    def setUp(self):
        self.response = ARAXResponse()
        self.response.debug("And we are off")
        self.response.info("So far so good")
        self.response.warning("This does not look good")
        self.response.error("Bad news, Pal", code="BadNewsError")

    def test_n_errors(self):
        self.assertEqual(self.response.n_errors, 1)

    def test_n_warnings(self):
        self.assertEqual(self.response.n_warnings, 1)

    def test_n_messages(self):
        self.assertEqual(self.response.n_messages, 4)

    def test_messages_list(self):
        self.assertEqual(len(self.response.messages_list(level=self.response.DEBUG)), 4)

    def test_status(self):
        self.assertEqual(self.response.status, "ERROR")

    def test_show(self):
        self.assertGreater(len(self.response.show(level=self.response.INFO)), 285)
