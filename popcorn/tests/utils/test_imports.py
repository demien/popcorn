import unittest
from popcorn.utils.imports import call_callable


class TestImports(unittest.TestCase):

    def test_class(self):
        obj = call_callable('popcorn.tests.utils.test_imports:Callable')
        self.assertEqual(str(obj), str(Callable()))

    def test_staticmethod(self):
        foo = 'foo'
        call_return = call_callable('popcorn.tests.utils.test_imports:Callable.echo', foo)
        self.assertEqual(foo, call_return)

    def test_function(self):
        foo = 'foo'
        call_return = call_callable('popcorn.tests.utils.test_imports.echo', foo)
        self.assertEqual(foo, call_return)

    def test_invalid_path(self):
        with self.assertRaises(ImportError):
            call_callable('popcorn.tests.utils.test_imports_fake.echo')

    def test_missing_arg(self):
        with self.assertRaises(TypeError):
            call_callable('popcorn.tests.utils.test_imports.echo')


class Callable(object):

    def __str__(self):
        return 'callable class toString'

    @staticmethod
    def echo(foo):
        return foo

def echo(foo):
    return foo
