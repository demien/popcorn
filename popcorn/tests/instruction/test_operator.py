import unittest
from popcorn.apps.exceptions import OperatorApplyException
from popcorn.apps.hub.order.operators import Operator


class TestOperator(unittest.TestCase):

    def test_inc(self):
        self.assertEqual(Operator('+').apply(1, 2), 1 + 2)
        self.assertEqual(Operator('+').slug, '+')

    def test_dec(self):
        self.assertEqual(Operator('-').apply(1, 2), 1 - 2)
        self.assertEqual(Operator('-').slug, '-')

    def test_to(self):
        self.assertEqual(Operator('=').apply(1, 2), 2)
        self.assertEqual(Operator('=').slug, '=')

    def test_all(self):
        self.assertEqual(len(Operator.ALL), 3)

    def test_invalid_apply(self):
        self.assertRaises(OperatorApplyException, Operator('+').apply, 1)

    def test_eqal(self):
        self.assertTrue(Operator('+') == Operator('+'))
        self.assertFalse(Operator('+') == Operator('-'))
