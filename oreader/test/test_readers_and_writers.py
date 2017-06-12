from oreader.base import DataObject, schema, IntegerColumn, StringColumn,\
    RealColumn, DateColumn, backrelate, relate
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.schema import MetaData, Table, Column
from sqlalchemy.sql.sqltypes import Integer, String, Float, Date
from oreader.writer_configs import SqaWriterConfig
import names
import random
import numpy as np
import datetime
from oreader.reader_configs import SqaReaderConfig
from itertools import islice, chain, cycle
import unittest
from frozendict import frozendict
from nose.tools import assert_list_equal, assert_raises, assert_equal, eq_
from oreader.groups import create_attribute_group_mixin,\
    AttributeGroup, AttributeGroupList

def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))

def create_sqa_connection_handling_case(engine_wrapper, assertion, reader_args={}):
    '''
    Generates a test function for testing an assertion about the behavior of 
    the reader after the engine has been modified by engine_wrapper.  The intentions is that 
    the engine_wrapper does something to make the data source unreliable and the
    assertion ensures that the reader handles the situation correctly.
    '''
    
    def tester():
        class FooObj(DataObject):
            partition_attribute = 'id'
        
        @schema([IntegerColumn(name='id')])
        class Foo(FooObj):
            identity_key_ = (('id', 'id'),)
            sort_key_ = ('id',)
        
        # Create a test database and table
        engine = create_engine('sqlite://')
        metadata = MetaData(bind=engine)
        foos_table = Foo.to_sqa_table(metadata, 'foos')
        metadata.create_all()
        
        # Define the mapping between tables and objects for writing
        writer_config = {Foo: SqaWriterConfig(foos_table),
                        }
        
        # Define the mapping between tables and objects for reading
        reader_config = {Foo: SqaReaderConfig(foos_table, engine_wrapper(engine), **reader_args)}
        
        # Create some objects
        foos = [Foo(id=i) for i in range(1000)]
        
        # Write the objects to the database
        writer = Foo.writer(writer_config)
        for foo in foos:
            writer.write(foo)
        
        def do_the_reading():
            return [foo for foo in Foo.reader(reader_config)]
        
        assertion(do_the_reading)
    
    return tester

class FaultyResultProxy(object):
    def __init__(self, result_proxy, failure_generator):
        self.result_proxy = result_proxy
        self.failure_generator = failure_generator
    
    def fetchone(self, *args, **kwargs):
        exc = self.failure_generator.next()
        if exc:
            raise exc 
        return self.result_proxy.fetchone(*args, **kwargs)

    def fetchmany(self, *args, **kwargs):
        exc = self.failure_generator.next()
        if exc:
            raise exc 
        return self.result_proxy.fetchmany(*args, **kwargs)
    
    def fetchall(self, *args, **kwargs):
        exc = self.failure_generator.next()
        if exc:
            raise exc 
        return self.result_proxy.fetchall(*args, **kwargs)
    
class FaultyEngine(object):
    def __init__(self, engine, failure_generator, proxy_failure_generator_generator):
        self.engine = engine
        self.failure_generator = failure_generator
        self.proxy_failure_generator_generator = proxy_failure_generator_generator

    def execute(self, *args, **kwargs):
        exc = self.failure_generator.next()
        if exc:
            raise exc 
        return FaultyResultProxy(self.engine.execute(*args, **kwargs), self.proxy_failure_generator_generator.next())

def fail_every_n_after_m(m, n, exc_func):
    i = 1
    while True:
        if i > m and (i-m) % n == 0:
            yield exc_func()
        else:
            yield False
        i += 1

def never_fail():
    while True:
        yield False

# This initial result proxy fails after 10 successful reads.  Subsequent attempts to create a new result proxy from the engine fail.
# This results in a ValueError after n_tries has been exhausted.
test_failed_sqa_connection = create_sqa_connection_handling_case(lambda eng: FaultyEngine(eng, fail_every_n_after_m(1, 1, ValueError), 
                                                                                    chain([fail_every_n_after_m(10, 1, ValueError)], 
                                                                                          cycle([never_fail()]))),
                                                           lambda read: assert_raises(ValueError, read),
                                                           {'n_tries': 5})
    
# This engine never fails to connect, but the resulting result proxies fail every 10 rows.  The reader should be able to handle this failure
# rate without a problem.
test_unreliable_sqa_connection = create_sqa_connection_handling_case(lambda eng: FaultyEngine(eng, never_fail(), 
                                                                                    cycle([fail_every_n_after_m(0, 10, ValueError)])),
                                                           lambda read: [assert_equal(foo.id, i) for i, foo in enumerate(read())],
                                                           {'n_tries': 5})
    
# This engine fails to connect every 5 tries. The result proxies fail every 10 rows.  The reader should be able to handle this failure
# rate without a problem.
test_unreliable_sqa_engine = create_sqa_connection_handling_case(lambda eng: FaultyEngine(eng, fail_every_n_after_m(0, 5, ValueError), 
                                                                                    cycle([fail_every_n_after_m(0, 10, ValueError)])),
                                                           lambda read: [assert_equal(foo.id, i) for i, foo in enumerate(read())],
                                                           {'n_tries': 5})

# Fail on engine
test_sqa_engine_fail = create_sqa_connection_handling_case(lambda eng: FaultyEngine(eng, fail_every_n_after_m(1, 1, TypeError), 
                                                                                    cycle([fail_every_n_after_m(0, 10, ValueError)])),
                                                           lambda read: assert_raises(TypeError, read),
                                                           {'n_tries': 5})

# Fail on proxy
test_sqa_proxy_fail = create_sqa_connection_handling_case(lambda eng: FaultyEngine(eng, never_fail(), 
                                                                                    chain([fail_every_n_after_m(0, 10, ValueError)],
                                                                                          cycle([fail_every_n_after_m(0, 1, TypeError)]))),
                                                           lambda read: assert_raises(TypeError, read),
                                                           {'n_tries': 5})


def test_attribute_groups():
    class HCObj(DataObject):
        partition_attribute = 'member_id'
    
    @schema([IntegerColumn(name='member_id'), StringColumn(name='name'), DateColumn(name='date_of_birth')])
    class Member(HCObj):
        identity_key_ = (('member_id', 'member_id'),)
        sort_key_ = ('member_id',)
        container_key_ = (('member_id', 'member_id'),)
        
        @classmethod
        def random(cls, parent):
            obj = cls()
            obj.member_id = parent
            obj.name = names.get_full_name()
            obj.date_of_birth = datetime.date(1940, 1, 1) + datetime.timedelta(days=np.random.geometric(.00001))
            n_claims = np.random.geometric(.1)
            obj.claims = []
            for _ in range(n_claims):
                obj.claims.append(cls.relationships['claims'][0].random(obj))
            return obj
    
    class RandomMemberFactory(object):
        def __iter__(self):
            i = 0
            while True:
                yield Member.random(i)
                i += 1
            
    class Procedure(object):
        def __init__(self, code, amount):
            self.code = code
            self.amount = amount
        
        def __nonzero__(self):
            return self.code is not None
    
    px_codes = [str(i) for i in range(10000)]
    
    PxMixIn = create_attribute_group_mixin('PxMixIn', 
                                 {'procedures': 
                                  AttributeGroupList([AttributeGroup(Procedure, {'code': 'px%d' % i,
                                                                                 'amount': 'px%d_amount' % i})
                                                      for i in range(1,4)])})
    
    @schema([IntegerColumn(name='member_id'), IntegerColumn(name='claim_id'), StringColumn(name='px1'),
             StringColumn(name='px2'), StringColumn(name='px3'), RealColumn(name='px1_amount'), 
             RealColumn(name='px2_amount'), RealColumn(name='px3_amount')])
    @backrelate({'claims': (Member, True)})
    class Claim(HCObj, PxMixIn):
        identity_key_ = (('member_id', 'member_id'), ('claim_id', 'claim_id'))
        sort_key_ = ('member_id', 'claim_id')
        container_key_ = (('member_id', 'member_id'),)
        
        @classmethod
        def random(cls, parent):
            obj = cls()
            obj.set_container_key(parent.identity_key())
            if parent.claims:
                obj.claim_id = max([c.claim_id for c in parent.claims]) + 1
            else:
                obj.claim_id = 0
            for i in range(1, random.choice([2,3,4])):
                setattr(obj, 'px%d' % i, random.choice(px_codes))
                setattr(obj, 'px%d_amount' % i, np.random.lognormal(mean=10., sigma=4.))
            return obj
    
    members = take(3, RandomMemberFactory())
    for member in members:
        for claim in member.claims:
            assert len(claim.procedures) in {1, 2, 3}
            for px in claim.procedures:
                assert type(px.code) is str
                assert type(px.amount) is float

def test_read_write():
    class EduObj(DataObject):
        partition_attribute = 'school_id'
    
    class SchoolParent(object):
        def __init__(self, school_id):
            self.school_id = school_id

    @schema([IntegerColumn(name='id'), StringColumn(name='name')])
    class School(EduObj):
        partition_attribute = 'id'
        identity_key_ = (('school_id', 'id'),)
        sort_key_ = ('id',)
        container_key_ = (('school_id', 'id'),)
        
        @classmethod
        def random(cls, parent):
            obj = cls()
            obj.id = parent.school_id
            obj.name = names.get_full_name() + ' ' + random.choice(['Elementary', 'Middle', 'High'])
            n_students = np.random.geometric(1/100.)
            n_employees = np.random.binomial(n_students, .1)
            obj.employees = []
            for _ in range(n_employees):
                obj.employees.append(cls.relationships['employees'][0].random(obj))
                
            n_contractors = np.random.geometric(1/10.)
            obj.contractors = []
            for _ in range(n_contractors):
                obj.contractors.append(cls.relationships['contractors'][0].random(obj))
            return obj

    class RandomSchoolFactory(object):
        def __iter__(self):
            school_id = 0
            while True:
                yield School.random(SchoolParent(school_id=school_id))
                school_id += 1
    
    @backrelate({'employees': (School, True)})
    class Employee(EduObj):
        @classmethod
        def random(cls, parent):
            klass = random.choice(cls.__subclasses__())
            return klass.random(parent)
    
    @schema([IntegerColumn(name='school_id'), IntegerColumn(name='id'), StringColumn(name='name')])
    class Teacher(Employee):
        identity_key_ = (('school_id', 'school_id'), ('teacher_id', 'id'))
        sort_key_ = ('school_id', 'id')
        container_key_ = (('school_id', 'school_id'),)
        @classmethod
        def random(cls, parent):
            obj = cls()
            obj.set_container_key(parent.identity_key())
            obj.name = names.get_full_name()
            if parent.employees:
                obj.id = max([e.id for e in parent.employees]) + 1
            else:
                obj.id = 0
            n_students = np.random.geometric(1/10.)
            obj.students = []
            for _ in range(n_students):
                obj.students.append(cls.relationships['students'][0].random(obj))
            return obj
            
    @schema([IntegerColumn(name='school_id'), IntegerColumn(name='id'), StringColumn(name='name')])
    class Administrator(Employee):
        identity_key_ = (('school_id', 'school_id'), ('administrator_id', 'id'))
        sort_key_ = ('school_id', 'id')
        container_key_ = (('school_id', 'school_id'),)
        
        @classmethod
        def random(cls, parent):
            obj = cls()
            obj.set_container_key(parent.identity_key())
            obj.name = names.get_full_name()
            if parent.employees:
                obj.id = max([e.id for e in parent.employees]) + 1
            else:
                obj.id = 0
            return obj

    @schema([IntegerColumn(name='school_id'), IntegerColumn(name='teacher_id'), IntegerColumn(name='id'), StringColumn(name='name')])
    @backrelate({'students': (Teacher, True)})
    class Student(EduObj):
        identity_key_ = (('school_id', 'school_id'), ('teacher_id', 'teacher_id'), ('id', 'id'))
        sort_key_ = ('school_id', 'teacher_id', 'id')
        container_key_ = (('school_id', 'school_id'), ('teacher_id', 'teacher_id'))

        @classmethod
        def random(cls, parent):
            obj = cls()
            obj.set_container_key(parent.identity_key())
            obj.name = names.get_full_name()
            if parent.students:
                obj.id = max([s.id for s in parent.students]) + 1
            else:
                obj.id = 0
            return obj
    
    @schema([IntegerColumn(name='school_id'), IntegerColumn(name='id'), StringColumn(name='name')])
    @backrelate({'contractors': (School, True)})
    class Contractor(EduObj):
        identity_key_ = (('school_id', 'school_id'), ('contractor_id', 'id'), ('contractor_name', 'name'))
        sort_key_ = ('school_id', 'id', 'name')
        container_key_ = (('school_id', 'school_id'),)
        @classmethod
        def random(cls, parent):
            obj = cls()
            obj.set_container_key(parent.identity_key())
            obj.name = names.get_full_name()
            if parent.contractors:
                obj.id = max([c.id for c in parent.contractors]) + 1
            else:
                obj.id = 0
            n_invoices = np.random.geometric(.1)
            obj.invoices = []
            for _ in range(n_invoices):
                obj.invoices.append(cls.relationships['invoices'][0].random(obj))
            return obj
        
    @schema([IntegerColumn(name='school_id'), IntegerColumn(name='contractor_id'), StringColumn(name='contractor_name'), 
             IntegerColumn(name='id'), DateColumn(name='date'), RealColumn(name='amount')])
    @backrelate({'invoices': (Contractor, True)})
    class Invoice(EduObj):
        identity_key_ = (('school_id', 'school_id'), ('contractor_id', 'contractor_id'), ('contractor_name', 'contractor_name'), ('id', 'id'))
        sort_key_ = ('school_id', 'contractor_id', 'contractor_name', 'id')
        container_key_ = (('school_id', 'school_id'), ('contractor_id', 'contractor_id'), ('contractor_name', 'contractor_name'))

        @classmethod
        def random(cls, parent):
            obj = cls()
            obj.set_container_key(parent.identity_key())
            if parent.invoices:
                obj.id = max([c.id for c in parent.invoices]) + 1
            else:
                obj.id = 0
            obj.amount = np.random.lognormal(mean=15., sigma=5.)
            date_offset = datetime.timedelta(days=np.random.geometric(.01))
            if parent.invoices:
                obj.date = max([c.date for c in parent.invoices]) + date_offset
            else:
                obj.date = datetime.date(1970, 1, 1) + date_offset + datetime.timedelta(days=np.random.geometric(.0002))
            return obj
    
    # Create in memory sqlite database with correct tables
    engine = create_engine('sqlite://')
    metadata = MetaData(bind=engine)
    
    
    schools_table = School.to_sqa_table(metadata, 'schools')
    teachers_table = Teacher.to_sqa_table(metadata, 'teachers')
    administrators_table = Administrator.to_sqa_table(metadata, 'administrators')
    students_table = Student.to_sqa_table(metadata, 'students')
    invoices_table = Invoice.to_sqa_table(metadata, 'invoices')
#     metadata.create_all()
    
    # Define the mapping between tables and objects for writing
    writer_config = {School: SqaWriterConfig(schools_table, create_table_if_not_exist=True),
                     Teacher: SqaWriterConfig(teachers_table, create_table_if_not_exist=True),
                     Administrator: SqaWriterConfig(administrators_table, create_table_if_not_exist=True),
                     Student: SqaWriterConfig(students_table, create_table_if_not_exist=True),
                     Invoice: SqaWriterConfig(invoices_table, create_table_if_not_exist=True)}
    
    # Define the mapping between tables and objects for reading
    reader_config = {School: SqaReaderConfig(schools_table, engine),
                     Teacher: SqaReaderConfig(teachers_table, engine),
                     Administrator: SqaReaderConfig(administrators_table, engine),
                     Student: SqaReaderConfig(students_table, engine),
                     Invoice: SqaReaderConfig(invoices_table, engine)}
    
    # Generate some random data
    schools = take(2, RandomSchoolFactory())
    writer = School.writer(writer_config)
    for school in schools:
        writer.write(school)
    reader = School.reader(reader_config)
    read_schools = list(reader)
    assert_list_equal(schools, read_schools)


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

if __name__ == '__main__':
    # This code will run the test in this file.'
    import sys
    import nose
    module_name = sys.modules[__name__].__file__

    result = nose.run(argv=[sys.argv[0],
                            module_name,
                            '-s','-v'])
    
        