from nose.tools import eq_, raises
import unittest
from frozendict import frozendict
from oreader.base import DataObject, relate, backrelate
from oreader import tools

class TestRelationships(unittest.TestCase):

    def test_relate(self):
        
        class A(DataObject):
            pass
        
        @relate({'a':(A,True)})
        class B(DataObject):
            pass
        
        class C(B):
            pass
        
        class D(A):
            pass
        
        eq_(B.relationships, frozendict({'a':(A,True)}))
        eq_(A.relationships, frozendict({}))
        eq_(C.relationships, frozendict({'a':(A,True)}))
        eq_(D.relationships, frozendict({}))
    
    def test_backrelate(self):
        class A(DataObject):
            pass
        
        @backrelate({'b':(A,True)})
        class B(DataObject):
            pass
        
        class C(B):
            pass
        
        class D(A):
            pass
        
        eq_(B.relationships, frozendict({}))
        eq_(A.relationships, frozendict({'b':(B,True)}))
        eq_(C.relationships, frozendict({}))
        eq_(D.relationships, frozendict({'b':(B,True)}))

class Obj:
    def __init__(self, **kwargs):
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

def test_all_or_none():
    eq_(tools.all_or_none([Obj(at=1), Obj(at=1)], 'at'), 1)
    eq_(tools.all_or_none([Obj(at=1), Obj(at=1)], 'attribute'), None)

    eq_(tools.all_or_none([Obj(at=1), Obj(at=2)], 'at'), None)
    eq_(tools.all_or_none([Obj(at=1), Obj(at=2)], 'attribute'), None)

    eq_(tools.all_or_none([Obj(at=1), Obj(at1=2)], 'at'), 1)
    eq_(tools.all_or_none([Obj(at=1), Obj(at1=2)], 'attribute'), None)


class test_all_or_raise:
    eq_(tools.all_or_raise([Obj(at=1), Obj(at=1)], 'at'), 1)
    eq_(tools.all_or_raise([Obj(at=1), Obj(at=1)], 'attribute'), None)

    @raises(ValueError)
    def test_error(self):
        tools.all_or_raise([Obj(at=1), Obj(at=2)], 'at')

    eq_(tools.all_or_raise([Obj(at=1), Obj(at=2)], 'attribute'), None)

    eq_(tools.all_or_raise([Obj(at=1), Obj(at1=2)], 'at'), 1)
    eq_(tools.all_or_raise([Obj(at=1), Obj(at1=2)], 'attribute'), None)


def test_mode():
    eq_(tools.mode([Obj(at='alpha'), Obj(at='omega'), Obj(at='alpha')], 'at'), 'alpha')
    eq_(tools.mode([Obj(at='alpha'), Obj(at='omega'), Obj(at='beta')], 'at'), 'alpha')


def test_minimum():
    eq_(tools.minimum([Obj(at=3), Obj(at=2), Obj(at=1)], 'at'), 1)
    eq_(tools.minimum([Obj(at=3), Obj(at=2), Obj(at=1)], 'attribute'), None)
    eq_(tools.minimum([Obj(at=3), Obj(at=2), Obj(at1=1)], 'at'), 2)

def test_maximum():
    eq_(tools.maximum([Obj(at=3), Obj(at=2), Obj(at=1)], 'at'), 3)
    eq_(tools.maximum([Obj(at=3), Obj(at=2), Obj(at=1)], 'attribute'), None)
    eq_(tools.maximum([Obj(at=3), Obj(at=2), Obj(at1=1)], 'at'), 3)

def test_latest():
    eq_(tools.latest([Obj(at=3, date=1), Obj(at=2, date=2), Obj(at=1, date=3)], 'at', 'date'), 1)
    eq_(tools.latest([Obj(at=3, date=1), Obj(at=2, date=3), Obj(at=1, date=3)], 'at', 'date'), 2)

def test_earliest():
    eq_(tools.earliest([Obj(at=3, date=1), Obj(at=2, date=2), Obj(at=1, date=3)], 'at', 'date'), 3)
    eq_(tools.earliest([Obj(at=3, date=3), Obj(at=2, date=2), Obj(at=1, date=2)], 'at', 'date'), 2)

def test_total():
    eq_(tools.total([Obj(at=3), Obj(at=2), Obj(at=1)], 'at'), 6)
    eq_(tools.total([Obj(at=3), Obj(at=2), Obj(at=1)], 'attribute'), None)
    eq_(tools.total([Obj(at=3), Obj(at=2), Obj(at1=1)], 'at'), 5)

def test_concatenation():
    eq_(tools.total([Obj(at='3'), Obj(at='2'), Obj(at='1')], 'at'), '321')
    eq_(tools.total([Obj(at='3'), Obj(at='2'), Obj(at='1')], 'attribute'), None)
    eq_(tools.total([Obj(at='3'), Obj(at='2'), Obj(at1='1')], 'at'), '32')
