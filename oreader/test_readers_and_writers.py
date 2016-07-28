from oreader.base import DataObject, schema, IntegerColumn, StringColumn,\
    RealColumn, DateColumn, backrelate
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.schema import MetaData, Table, Column
from sqlalchemy.sql.sqltypes import Integer, String, Float, Date
from oreader.writer_configs import SqaWriterConfig
import names
import random
import numpy as np
import datetime
from oreader.reader_configs import SqaReaderConfig
from itertools import islice
from nose.tools import assert_list_equal


def test_read_write():
    class EduObj(DataObject):
        partition_attribute = 'school_id'
    
    class SchoolParent(object):
        def __init__(self, school_id):
            self.school_id = school_id
#         
#         identity_key_ = (('parent_id', 'parent_id'),)
#         def random(self, parent=None):
#     
#     class RandomSchoolFactory(object):
#         def __iter__(self):
#     
    @schema([IntegerColumn(name='id'), StringColumn(name='name')])
    class School(EduObj):
        partition_attribute = 'id'
        identity_key_ = (('school_id', 'id'),)
#         def identity_key(self):
#             return {'school_id': self.id}

        sort_key_ = ('id',)
        container_key_ = (('school_id', 'id'),)
#         def container_key(self):
#             return {'school_id': self.id}
        
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
#             n_teachers = np.random.binomial(n_students, .04)[0]
#             n_administrators = 2 * np.random.binomial(n_teachers, .5)
#             teachers = [cls.relationships['students'][0].random(obj) for _ in range(n_students)]
#             obj.students = [cls.relationships['students'][0].random(obj) for _ in range(n_students)]

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
    
#     @sub(Employee)
    @schema([IntegerColumn(name='school_id'), IntegerColumn(name='id'), StringColumn(name='name')])
    class Teacher(Employee):
        identity_key_ = (('school_id', 'school_id'), ('teacher_id', 'id'))
#         def identity_key(self):
#             return {'school_id': self.school_id,
#                     'id': self.id}

        sort_key_ = ('school_id', 'id')
        container_key_ = (('school_id', 'school_id'),)
#         def container_key(self):
#             return {'school_id': self.school_id}
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
            
#     @sub(Employee)
    @schema([IntegerColumn(name='school_id'), IntegerColumn(name='id'), StringColumn(name='name')])
    class Administrator(Employee):
        identity_key_ = (('school_id', 'school_id'), ('administrator_id', 'id'))
#         def identity_key(self):
#             return {'school_id': self.school_id,
#                     'id': self.id}

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
#         def container_key(self):
#             return {'school_id': self.school_id}
#     
    @schema([IntegerColumn(name='school_id'), IntegerColumn(name='teacher_id'), IntegerColumn(name='id'), StringColumn(name='name')])
    @backrelate({'students': (Teacher, True)})
    class Student(EduObj):
        identity_key_ = (('school_id', 'school_id'), ('teacher_id', 'teacher_id'), ('id', 'id'))
#         def identity_key(self):
#             return {'school_id': self.school_id,
#                     'teacher_id': self.teacher_id,
#                     'id': self.id}

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
        
#         def container_key(self):
#             return {'school_id': self.school_id,
#                     'teacher_id': self.teacher_id}
#         def __init__(self, id, name, grade):
#             self.id = id
#             self.name = name
#             self.grade = grade
    
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
#         def identity_key(self):
#             return {'school_id': self.school_id,
#                     'contractor_id': self.contractor_id,
#                     'id': self.id}

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
#         def container_key(self):
#             return {'school_id': self.school_id,
#                     'contractor_id': self.contractor_id}
    
    # Create in memory sqlite database with correct tables
    engine = create_engine('sqlite://')
    metadata = MetaData(bind=engine)
    def sqa_col(col):
        if isinstance(col, IntegerColumn):
            sqa_type = Integer()
        elif isinstance(col, StringColumn):
            sqa_type = String()
        elif isinstance(col, RealColumn):
            sqa_type = Float()
        elif isinstance(col, DateColumn):
            sqa_type = Date()
        name = col.name
        return Column(name, sqa_type)
    
    def table_from_class(klass, metadata, name):
        cols = [sqa_col(col) for col in klass.columns]
        return Table(name, metadata, *cols)
    
    schools_table = table_from_class(School, metadata, 'schools')
    teachers_table = table_from_class(Teacher, metadata, 'teachers')
    administrators_table = table_from_class(Administrator, metadata, 'administrators')
    students_table = table_from_class(Student, metadata, 'students')
    invoices_table = table_from_class(Invoice, metadata, 'invoices')
    metadata.create_all()
    
    # Define the mapping between tables and objects for writing
    writer_config = {School: SqaWriterConfig(schools_table),
                     Teacher: SqaWriterConfig(teachers_table),
                     Administrator: SqaWriterConfig(administrators_table),
                     Student: SqaWriterConfig(students_table),
                     Invoice: SqaWriterConfig(invoices_table)}
    
    # Define the mapping between tables and objects for reading
    reader_config = {School: SqaReaderConfig(schools_table, engine),
                     Teacher: SqaReaderConfig(teachers_table, engine),
                     Administrator: SqaReaderConfig(administrators_table, engine),
                     Student: SqaReaderConfig(students_table, engine),
                     Invoice: SqaReaderConfig(invoices_table, engine)}
    
    # Generate some random data
    def take(n, iterable):
        "Return first n items of the iterable as a list"
        return list(islice(iterable, n))
    schools = take(2, RandomSchoolFactory())
    writer = School.writer(writer_config)
    for school in schools:
        writer.write(school)
    reader = School.reader(reader_config)
    read_schools = list(reader)
    assert_list_equal(schools, read_schools)
    
if __name__ == '__main__':
    test_read_write()
    print 'Success!'

    
        